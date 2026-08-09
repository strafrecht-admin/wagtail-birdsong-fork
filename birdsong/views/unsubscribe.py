from types import SimpleNamespace

from django.conf import settings
from django.shortcuts import get_object_or_404, render
from django.views.decorators.http import require_http_methods
from wagtail.models import Site

from birdsong.models import Contact


@require_http_methods(["GET", "POST"])
def unsubscribe_user(request, user_id):
    """Unsubscribe a newsletter contact.

    GET  — render a confirmation page so email link-checkers / prefetchers
           cannot silently trigger the deletion.
    POST — perform the deletion with CSRF protection (the confirmation form
           includes {% csrf_token %}).
    """
    contact = get_object_or_404(Contact, id=user_id)
    site = Site.find_for_request(request)

    confirm_template = getattr(
        settings,
        'BIRDSONG_UNSUBSCRIBE_CONFIRM_TEMPLATE',
        'birdsong/unsubscribe_confirm.html',
    )
    done_template = getattr(
        settings,
        'BIRDSONG_UNSUBSCRIBE_TEMPLATE',
        'unsubscribe.html',
    )

    if request.method == "POST":
        # Save the email for the confirmation message before deleting.
        contact_email = contact.email
        contact.delete()
        return render(
            request, done_template, context={
                'site': site,
                'contact': SimpleNamespace(email=contact_email),
            }
        )

    # GET — show confirmation page
    return render(
        request, confirm_template, context={
            'site': site,
            'contact': contact,
        }
    )
