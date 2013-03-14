from tweepy import utils
from datetime import datetime

def parse_timestamp(timestamp_int):
    """convert unix timestamp to a datetime instance; raises TypeError"""
    return datetime.fromtimestamp(int(timestamp_int))

def process_entities(tweet_dict, options={}):
    """processes tweet_dict['text'] and tweet_dict['entities']. returns (html_text, xml_text) 
        see https://dev.twitter.com/docs/tweet-entities. available options are:
            html_photo_link: 'text', 'thumb'
            html_photo_size: 'large', 'small', 'thumb', etc
            html_prefer_http': True | False
            xml_full: True | False
    """
    def html_escape(text):
        """convert some html entities; http://bit.ly/ZlOkFX"""
        if not isinstance(text, basestring):
            return text
        html_escape_table = {
            '&': '&amp;',
            '"': '&quot;',
            "'": '&apos;',
            '>': '&gt;',
            '<': '&lt;',
        }
        return ''.join(html_escape_table.get(c,c) for c in text)
    
    def make_tag(tag_name, attrib_dict, text='', escape_text=True, short_tags=False):
        """generates a tag with or without content"""
        attrib_list = []
        for attrib in attrib_dict:
            if attrib_dict[attrib] is not None:
                attrib_list.append('%s="%s"' % (attrib, html_escape(attrib_dict[attrib])))
        attrib_string = ' ' + ' '.join(attrib_list)            
        if not text:
            if short_tags:
                return '<%s%s />' % (tag_name, attrib_string)
            else:
                return '<%s%s></%s>' % (tag_name, attrib_string, tag_name)
        else:
            text_inside= text if not escape_text else html_escape(text)
            return '<%s%s>%s</%s>' % (
                    tag_name, attrib_string, text_inside, tag_name
            )

    # -- begin: __process_entities --
    text = tweet_dict['text']
    xml_text = ''
    html_text = ''
    html_opts_photo_link = options.get('html_photo_link','text')  
    html_opts_photo_size = options.get('html_photo_size','large')  
    html_opts_prefer_https = options.get('html_prefer_https',True)        
    xml_opts_full_xml =  options.get('xml_full',False)        

    if 'entities' in tweet_dict and isinstance(tweet_dict['entities'], dict):
        output_list = []
        # create a flattened entities list so we can sort later
        ent_list= []
        for ent_type in tweet_dict['entities']:
            for ent_item in tweet_dict['entities'][ent_type]:
                ent_list.append({
                    'type': ent_type,
                    'item': ent_item,
                    'pos': ent_item['indices'][0] if 'indices' in ent_item else 0,
                    'pos_next': ent_item['indices'][1] if 'indices' in ent_item else 0,
                    'text': ''
                })

        # sort the list
        ent_list.sort(key=lambda x: x['pos'])
       
        # prepare content_list, which will be the final output in list format
        content_list= []
        idx = ent_idx = 0
        max_element_count = 99999
        while idx < len(text) and len(content_list) <= max_element_count: 
            if ent_idx < len(ent_list) and ent_list[ent_idx]['pos'] == idx:
                # next segment is an entity
                ent_list[ent_idx]['text'] = text[idx : ent_list[ent_idx]['pos_next']]
                content_list.append(ent_list[ent_idx])
                idx = ent_list[ent_idx]['pos_next']  
                ent_idx += 1
            else:
                # next segment is a string (by default, the segment spans till end of the text)
                string_end_idx = len(text) 
                if ent_idx < len(ent_list):
                    string_end_idx = ent_list[ent_idx]['pos'] 
                content_list.append(text[idx : string_end_idx])
                idx = string_end_idx 

        # concatenate each member in content_list 
        for content_list_item in content_list:
            if isinstance(content_list_item,basestring):
                html_text += content_list_item
                xml_text += content_list_item
            else:
                # is an entity
                entity = content_list_item['item']

                if content_list_item['type'] == 'media':
                    html_link_target = ''
                    thumb_img_src = ''

                    size_html_list= []
                    for size in entity.get('sizes', {}):
                        size_dict = entity['sizes'][size] 
                        url_https = '%s:%s' % (entity.get('media_url_https', ''), size)
                        url_http = '%s:%s' % (entity.get('media_url', ''), size)

                        size_html_list.append(make_tag('size', {
                            'key': size, 
                            'width': size_dict.get('w', 0), 
                            'height': size_dict.get('h', 0), 
                            'resize': size_dict.get('resize',''), 
                            'url': url_http,
                            'url_https': url_https,
                        }))
                        if size == html_opts_photo_size:
                            html_link_target = url_https if html_opts_prefer_https else url_http
                        if size == 'thumb':
                            thumb_img_src= url_https if html_opts_prefer_https else url_http

                    xml_text += make_tag('media', {
                        'type': entity.get('type', ''),
                        'id': entity.get('id', ''),
                        'url': entity.get('url', ''),
                        'display_url': entity.get('display_url', ''),
                        'expanded_url': entity.get('expanded_url', ''),
                    }, ''.join(size_html_list), escape_text=False)
                    
                    default_link = entity.get('media_url_https' if html_opts_prefer_https else 'media_url','#')
                    link_title = None 
                    link_text = None
                    escape_text = True 
                    if 'thumb' == html_opts_photo_link:
                        if thumb_img_src:
                            link_text = make_tag('img', {
                                'src': thumb_img_src,
                                'alt': entity.get('display_url', 'preview'),
                            }, short_tags=True)
                            escape_text = False 
                        else:
                            # no thumbnail image available
                            link_text = entity.get('display_url','')
                            link_title = 'no thumbnail available'
                    else:
                        link_text = entity.get('display_url','')

                    html_text += make_tag('a', {
                        'class': ('media %s' % entity.get('type','')).strip(),
                        'href': html_link_target if html_link_target else default_link,
                        'title': link_title,
                    }, link_text, escape_text)

                elif content_list_item['type'] == 'urls':
                    xml_text += make_tag('link', {
                        'url': entity.get('url', ''),
                        'display_url': entity.get('display_url', ''),
                        'expanded_url': entity.get('expanded_url', ''),
                    }, content_list_item['text'])
                    
                    html_text += make_tag('a', {
                        'class': 'link',
                        'href': entity.get('url', ''),
                        'alt': entity.get('expanded_url', ''),
                    }, content_list_item['text']);

                elif content_list_item['type'] == 'user_mentions':
                    xml_text += make_tag('mention', {
                        'user_id': entity.get('id', ''),
                        'screen_name': entity.get('screen_name', ''),
                        'name': entity.get('name', ''),
                    }, content_list_item['text'])
                    
                    html_text += make_tag('a', {
                        'class': 'mention',    
                        # the safest way to link to a user is by user ID (user can change screen names)
                        'href': '%s://twitter.com/account/redirect_by_id?id=%s' % (
                            'https' if html_opts_prefer_https else 'http', entity.get('id', '')
                        ),
                        'alt': '%s://twitter.com/%s' % (
                            'https' if html_opts_prefer_https else 'http', urllib.quote(entity.get('screen_name', ''))
                        )                                
                    }, content_list_item['text'])

                elif content_list_item['type'] == 'hashtags':
                    xml_text += make_tag('hashtag', {
                        'text': entity.get('text', ''),
                    }, content_list_item['text'])
                    
                    html_text += make_tag('a', {
                        'class': 'hashtag',    
                        'href': '%s://twitter.com/search?%s' % (
                            'https' if html_opts_prefer_https else 'http', urllib.urlencode([('q','#'+entity.get('text',''))])
                        )
                    }, content_list_item['text'])

                else:
                    html_text += '<!-- unknown entity -->%s<!-- end -->' % html_escape(content_list_item['text'])
                    xml_text += make_tag('entity', {
                        'type': content_list_item['type'], 
                        'content': content_list_item['item'], 
                    }, content_list_item['text'])

            
        return (
                html_text,
                '<?xml version="1.0" enoding="utf-8" ?><tweet>%s</tweet>' % xml_text if xml_opts_full_xml else xml_text
        )
    else:
        # no entities found
        html_text= tweet_dict['text']
        xml_text= tweet_dict['text'] 

    # html: <a href="expanded_url" alt="url">url</a>
    return (html_text, xml_text)

