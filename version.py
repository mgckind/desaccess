"""easyweb version"""

version_tag = (1, 0, 0, 'dev-b84b8af')
__version__ = '.'.join(map(str, version_tag[:3]))

if len(version_tag) > 3:
    __version__ = '%s-%s' % (__version__, version_tag[3])
