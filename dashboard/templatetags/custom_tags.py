from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """在模板中通过 key 访问字典的值"""
    if isinstance(dictionary, dict):
        return dictionary.get(key, "")
    return ""
