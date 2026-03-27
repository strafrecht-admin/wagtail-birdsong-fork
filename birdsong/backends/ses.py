import logging
import time
from threading import Thread

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from django.conf import settings
from django.db import close_old_connections, transaction
from django.template.exceptions import TemplateDoesNotExist
from django.template.loader import render_to_string
from django.utils import timezone

from birdsong.models import Campaign, CampaignStatus, Contact

from . import BaseEmailBackend

logger = logging.getLogger(__name__)


class SESCampaignThread(Thread):
    """Background thread for sending campaigns via AWS SES"""
    
    def __init__(self, campaign_pk, contact_pks, messages):
        super().__init__()
        self.campaign_pk = campaign_pk
        self.contact_pks = contact_pks
        self.messages = messages
        
    def _get_ses_client(self):
        """Create and return AWS SES client"""
        session = boto3.Session(
            aws_access_key_id=getattr(settings, 'AWS_ACCESS_KEY_ID', None),
            aws_secret_access_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', None),
            region_name=getattr(settings, 'AWS_SES_REGION', 'eu-west-1')
        )
        return session.client('ses')
    
    def run(self):
        """Execute the campaign sending in background"""
        campaign = Campaign.objects.get(pk=self.campaign_pk)
        ses_client = self._get_ses_client()
        
        # Get configuration
        batch_size = getattr(settings, 'AWS_SES_BATCH_SIZE', 10)
        rate_limit = getattr(settings, 'AWS_SES_RATE_LIMIT', 14)  # emails per second
        
        try:
            logger.info(f"Sending {len(self.messages)} emails via AWS SES")
            
            sent_count = 0
            failed_count = 0
            
            # Process messages in batches
            for i in range(0, len(self.messages), batch_size):
                batch = self.messages[i:i + batch_size]
                batch_start_time = time.time()
                
                for message in batch:
                    try:
                        # Send email via SES
                        response = ses_client.send_email(
                            Source=message['from_email'],
                            Destination={
                                'ToAddresses': message['to']
                            },
                            Message={
                                'Subject': {
                                    'Data': message['subject'],
                                    'Charset': 'UTF-8'
                                },
                                'Body': {
                                    'Html': {
                                        'Data': message.get('html_body', message.get('body', '')),
                                        'Charset': 'UTF-8'
                                    },
                                    'Text': {
                                        'Data': message.get('body', message.get('html_body', '')),
                                        'Charset': 'UTF-8'
                                    }
                                }
                            },
                            ReplyToAddresses=message.get('reply_to', [])
                        )
                        sent_count += 1
                        logger.debug(f"Email sent successfully: MessageId={response.get('MessageId')}")
                        
                    except (ClientError, BotoCoreError) as e:
                        failed_count += 1
                        logger.error(f"Failed to send email to {message['to']}: {str(e)}")
                        # Continue with next message instead of failing entire campaign
                        continue
                
                # Rate limiting: ensure we don't exceed rate limit
                batch_duration = time.time() - batch_start_time
                emails_per_second = len(batch) / max(batch_duration, 0.001)
                
                if emails_per_second > rate_limit and i + batch_size < len(self.messages):
                    # Calculate sleep time to stay within rate limit
                    required_time = len(batch) / rate_limit
                    sleep_time = required_time - batch_duration
                    if sleep_time > 0:
                        logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                        time.sleep(sleep_time)
            
            logger.info(f"Campaign sending completed: {sent_count} sent, {failed_count} failed")
            
            # Update campaign status
            with transaction.atomic():
                if failed_count == 0:
                    campaign.status = CampaignStatus.SENT
                    campaign.sent_date = timezone.now()
                elif sent_count > 0:
                    # Partial success - still mark as sent
                    campaign.status = CampaignStatus.SENT
                    campaign.sent_date = timezone.now()
                    logger.warning(f"Campaign {self.campaign_pk} had {failed_count} failures")
                else:
                    # Complete failure
                    campaign.status = CampaignStatus.FAILED
                    logger.error(f"Campaign {self.campaign_pk} completely failed")
                
                campaign.save()
                
                # Add successfully contacted recipients
                if sent_count > 0:
                    fresh_contacts = Contact.objects.filter(pk__in=self.contact_pks)
                    campaign.receipts.add(*fresh_contacts)
                    
        except Exception as e:
            logger.exception(f"Unexpected error sending campaign {self.campaign_pk}: {str(e)}")
            campaign.status = CampaignStatus.FAILED
            campaign.save()
        finally:
            close_old_connections()


class SESEmailBackend(BaseEmailBackend):
    """AWS SES email backend for Birdsong newsletters"""
    
    def __init__(self):
        """Initialize SES backend"""
        super().__init__()
        
        # Validate AWS configuration
        if not getattr(settings, 'AWS_ACCESS_KEY_ID', None):
            logger.warning("AWS_ACCESS_KEY_ID not configured - SES emails may fail")
        if not getattr(settings, 'AWS_SECRET_ACCESS_KEY', None):
            logger.warning("AWS_SECRET_ACCESS_KEY not configured - SES emails may fail")
        
        # Log configuration
        region = getattr(settings, 'AWS_SES_REGION', 'eu-west-1')
        batch_size = getattr(settings, 'AWS_SES_BATCH_SIZE', 10)
        rate_limit = getattr(settings, 'AWS_SES_RATE_LIMIT', 14)
        logger.info(f"SES Backend initialized: region={region}, batch_size={batch_size}, rate_limit={rate_limit}")
    
    def send_campaign(self, request, campaign, contacts, test_send=False):
        """
        Send campaign to contacts via AWS SES
        
        Args:
            request: HTTP request object (can be None)
            campaign: Campaign model instance
            contacts: List/QuerySet of Contact objects
            test_send: If True, send synchronously without updating campaign status
        """
        messages = []
        
        for contact in contacts:
            message_data = {
                'subject': campaign.subject,
                'from_email': self.from_email,
                'to': [contact.email],
                'reply_to': [self.reply_to],
            }
            
            # Render HTML template
            html_content = render_to_string(
                campaign.get_template(request),
                campaign.get_context(request, contact),
            )
            
            # Try to render text template, fallback to HTML if not found
            try:
                text_content = render_to_string(
                    campaign.get_text_template(request),
                    campaign.get_context(request, contact),
                )
                message_data['body'] = text_content
                message_data['html_body'] = html_content
            except TemplateDoesNotExist:
                # Use HTML as fallback for both
                message_data['body'] = html_content
                message_data['html_body'] = html_content
            
            messages.append(message_data)
        
        if test_send:
            # Synchronous send (used by the Celery task which handles its own batching).
            # Returns a list of per-contact result dicts so the task can track individual
            # success/failure.  Errors are caught per-message — a suppressed address or
            # transient SES fault will NOT abort the rest of the batch.
            rate_limit = getattr(settings, 'AWS_SES_RATE_LIMIT', 14)
            logger.info(f"Sending campaign synchronously to {len(messages)} recipients (rate_limit={rate_limit}/sec)")
            ses_client = boto3.Session(
                aws_access_key_id=getattr(settings, 'AWS_ACCESS_KEY_ID', None),
                aws_secret_access_key=getattr(settings, 'AWS_SECRET_ACCESS_KEY', None),
                region_name=getattr(settings, 'AWS_SES_REGION', 'eu-west-1')
            ).client('ses')

            results = []
            batch_start = time.time()
            for i, message in enumerate(messages):
                try:
                    response = ses_client.send_email(
                        Source=message['from_email'],
                        Destination={'ToAddresses': message['to']},
                        Message={
                            'Subject': {'Data': message['subject'], 'Charset': 'UTF-8'},
                            'Body': {
                                'Html': {'Data': message.get('html_body', message.get('body', '')), 'Charset': 'UTF-8'},
                                'Text': {'Data': message.get('body', message.get('html_body', '')), 'Charset': 'UTF-8'},
                            },
                        },
                        ReplyToAddresses=message.get('reply_to', []),
                    )
                    msg_id = response.get('MessageId', '')
                    logger.debug(f"Sent to {message['to']}: MessageId={msg_id}")
                    results.append({'success': True, 'ses_message_id': msg_id, 'error_message': '', 'backend_response_code': 'ok'})
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"Failed to send to {message['to']}: {e}")
                    results.append({'success': False, 'ses_message_id': '', 'error_message': str(e), 'backend_response_code': 'ses_error'})

                # Rate limiting: after every email, throttle to stay within rate_limit/sec
                elapsed = time.time() - batch_start
                expected = (i + 1) / rate_limit
                if expected > elapsed:
                    time.sleep(expected - elapsed)

            sent = sum(1 for r in results if r['success'])
            failed = len(results) - sent
            logger.info(f"Batch complete: {sent} sent, {failed} failed")
            return results
        else:
            # Production mode: send in background thread
            campaign_thread = SESCampaignThread(
                campaign.pk, [c.pk for c in contacts], messages
            )
            campaign_thread.start()
            logger.info(f"Started background thread for campaign {campaign.pk}")
