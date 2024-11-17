import logging
import packaging
import re
import types
from enum import Enum

log = logging.getLogger('metatools.autogen')


class SortMethod(Enum):
    DATE = "DATE"
    VERSION = "VERSION"


class VersionMatch(Enum):
    """
    This Enum is used to collect our official regexes to match versions in strings, in the general case.
    STANDARD will match a regular version string like 0.5.3, etc. and will also support an optional
    _p(patchlevel) and -r(revision), which is compatible with both Gentoo/Funtoo and packaging.version
    sorting.

    Note that this regex is *not* anchored at all, so it can appear *anywhere* in the string. Because
    ``RegexMatcher`` uses re.search(), it will grab the first occurrence in the string, wherever it
    appears.
    """
    GRABBY = r'([\d.]+(?:_p\d+)?(?:-r\d+)?)'


class TagVersionMatch(Enum):
    """
    This Enum is used to collect our official regexes for matching versions in *tags*. GitHub tags
    typically have the format 1.2.3 or v1.2.3. the STANDARD regex will match these formats, but is
    anchored so will not match th numeric part of test_20220101.

    We used to use the GRABBY regex as default, which *would* match 20220101 in the regex above,
    as it is not anchored.

    This, we found, is likely not a great default regex so it was changed to STANDARD.
    """
    GRABBY = VersionMatch.GRABBY.value
    STANDARD = f"^v?{VersionMatch.GRABBY.value}$"


class ReleaseVersionMatch(Enum):
    """
    This Enum is used to collect our official regexes for matching versions in *releases*.

    For *releases*, we are a bit more ambitious due to the variety of GitHub release names, and we
    use the default "grabby" non-anchored regex.
    """
    STANDARD = VersionMatch.GRABBY.value


class Matcher:
    """
    Big picture: This class abstracts versioning handling, so we can have pluggable version handlers that
    have differing abilities to handle different types of versions.

    Details: This class lets us extract versions from strings, as well as sort them. It can be used to add more
    Functions in this generator accept a matcher= argument which is designed to allow customization of
    version-related functionality by passing a non-default matcher instance to the methods if desired.
    """

    def match(self, input: str):
        """
        This method should extract something from the input that resembles a version, and return the
        matching part, or None if no match was found.
        """
        pass

    def sortable(self, version):
        """
        This method should return a **sortable** representation of the version grabbed by the match()
        method, above.
        """
        pass


class RegexMatcher(Matcher):
    """
    This is the default matcher used by these functions.
    """

    regex = None

    def __repr__(self):
        return f"RegexMatcher({repr(self.regex)})"

    def __init__(self, regex=None, select=None, filter=None, transform=None):
        self.select = select
        self.filter = filter
        self.transform = transform

        if regex is None:
            if self.regex is None:
                self.regex = self.get_default_regex()
        else:
            if isinstance(regex, Enum):
                self.regex = re.compile(regex.value)
            elif isinstance(regex, re.Pattern):
                pass
            elif isinstance(regex, str):
                self.regex = re.compile(regex)
            else:
                raise ValueError(f"Unrecognized regex type: {type(regex)}")

    def get_default_regex(self):
        return re.compile(VersionMatch.GRABBY.value)

    def match(self, input: str, select=None, filter=None, transform=None):
        retval = self._match(input=input, select=select, filter=filter, transform=transform)
        log.debug(
            f"{__class__.__name__}: input: {repr(input)} select={repr(select)} filter={repr(filter)} transform={repr(transform)} retval={repr(retval)}")
        return retval

    def _match(self, input: str, select=None, filter=None, transform=None):

        if transform:
            input = transform(input)
        if select and not re.match(select, input):
            return None
        if filter:
            if isinstance(filter, str):
                if re.match(filter, input):
                    return None
            elif isinstance(filter, list):
                for each_filter in filter:
                    if re.match(each_filter, input):
                        return None
        match = self.regex.search(input)
        if match:
            return match.groups()[0]

    def sortable(self, version):
        # NOTE: Version must be valid for packaging version (24.x) to avoid
        #       exceptions. So for packages like openssh I replace
        #       postfix to manage it as build suffix after '+' char.

        # This is also possible... OMG... tag with version .1.3.2
        if version.startswith("."):
            version = version[1:len(version)]
        if "_p" in version:
            version = version.replace("_p", "+p")

        # Yeah, it's true there are packages with
        # mad versions
        if "--" in version:
            version = version.replace("--", "+")
        if "-" in version:
            version = version.replace("-", "+")

        return packaging.version.parse(version)


class TagRegexMatcher(RegexMatcher):

    def get_default_regex(self):
        if self.filter or self.select:
            return re.compile(TagVersionMatch.GRABBY.value)
        else:
            return re.compile(TagVersionMatch.STANDARD.value)


class ReleaseRegexMatcher(RegexMatcher):
    regex = re.compile(ReleaseVersionMatch.STANDARD.value)


async def iter_tag_versions(tags_list, select=None, filter=None, matcher=None, transform=None, version=None):
    """
    This method iterates over each tag in tags_list, extracts the version information, and
    yields a tuple of that version as well as the entire GitHub tag data for that tag.

    ``select`` specifies a regex string that must match for the tag version to be considered.

    ``filter`` can be either a regex string or a list of regex strings. Anything that matches
    this string or strings will be excluded.

    ``version``, if specified, is a specific version we want. If not specified, all versions
    will be returned.

    ``transform`` is a lambda/single-argument function that if specified will be used to
    arbitrarily modify the tag before it is searched for versions, or for the ``select``
    regex.

    ``matcher`` is an optional function that accepts a single argument of the tag we are
    processing. By default we will use the ``regex_matcher`` to search for a basic version
    pattern somewhere within the tag.
    """
    if matcher is None:
        matcher = TagRegexMatcher(select=select, filter=filter)

    # To simplify the code, if we don't get tags_list as an async generator, let's just wrap our list so
    # that it's in an async generator. This may be unnecessary, but need to do some testing
    # to see, so let's just do it this way for now:

    if not isinstance(tags_list, types.AsyncGeneratorType):
        async def tags_list_gen(t_list):
            for tag in t_list:
                yield tag

        tags_gen = tags_list_gen(tags_list)
    else:
        # We already have an async generator:
        tags_gen = tags_list

    async for tag_data in tags_gen:
        match = matcher.match(tag_data['name'], select=select, filter=filter, transform=transform)
        if match:
            if version:
                if match != version:
                    continue
            yield match, tag_data


def create_transform(transform_data):
    def transform_lambda(tag):
        for trans_dict in transform_data:
            if "kind" not in trans_dict:
                raise ValueError("Please specify 'kind' for github transform: element.")
            kind = trans_dict['kind']
            if kind == "string":
                match = trans_dict['match']
                replace = trans_dict['replace']
                tag = tag.replace(match, replace)
            else:
                raise ValueError(f"Unknown 'kind' for github transform: {kind}")
        return tag

    return transform_lambda
