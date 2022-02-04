from nostril import nonsense

s = 'https://docs.google.com/forms/d/e/1FAIpQLSew3gtG3kLZYHeQK1KPSx-GrsgSOSA-_XgSR8rm8lrZyduqBw/viewform'
s = [x for x in s.split('/') if len(x)>5];

for part in s:
    print('{}: {}'.format(part, 'nonsense' if nonsense(part) else 'real'))
