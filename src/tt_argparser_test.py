#!env python

from tt2 import CArgParser

p = CArgParser(param_def={    
        'foo   do some foo-ish things': [
            ('<twice>x2  here is some help for you', str),
            ('[names]...  here is some help for you', str),
            ('<--city,-c=city name>  where do you live?', str), 
            ('<--postcode,-p=nnn-nnnn>  post code, must be in form nnn-nnnn', '([0-9]{3})\-([0-9]{4})'), 
            ('<--id,-i=customer ID>', int), 
            ('<filename>x2', str),  # match twice; will give p['filename'] = ['ham', 'spam']
        ],
        'bar   go to a bar and hang out with friends': [
            ('<profile_alias>', str),
            ('[--pin,-i]', int, 'verification code given by Twitter. Omit if you don\'t have one', None),
        ],
     }, default_action='foo', tolerate_regex_mismatch=False)

print p.parse()



