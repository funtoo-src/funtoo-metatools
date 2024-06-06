# tag_gen and release_gen are higher-level functions that return a dict of items, suitable for
# augmenting the pkginfo dict, and thus easy to integrate into yaml-based autogens.
from collections import defaultdict

from metatools.generator.transform import SortMethod, ReleaseRegexMatcher, TagRegexMatcher, RegexMatcher, \
    iter_tag_versions


def factor_filters(include):
    """
	By default, the ``release_gen`` method below filters prereleases and drafts. It's now possible
	to disable this by specifying an 'include=' keyword argument to either function, which can be a
	set or a list containing one or more of the following strings:

	* "prerelease"
	* "draft"

	Given this include list/set as an argument, this method will return the set of values that should
	be checked and skipped if found to be True in the JSON (the inverse set, which is easier to use
	in the ``release_gen`` loop when we check.)
	"""

    valid_filters = {"prerelease", "draft"}

    if include is not None:
        for item in include:
            if item not in valid_filters:
                raise ValueError(f"release_gen include= option of '{item}' is not recognized")
        include = set(include)
    else:
        include = set()

    return valid_filters - include


def fetch_release_data(hub, github_user, github_repo):
    return hub.pkgtools.fetch.get_page(
        f"https://api.github.com/repos/{github_user}/{github_repo}/releases?per_page=100", is_json=True)


def fetch_tag_data(hub, github_user, github_repo):
    return hub.pkgtools.fetch.get_page(f"https://api.github.com/repos/{github_user}/{github_repo}/tags?per_page=100",
                                       is_json=True)


def fetch_ref(hub, github_user, github_repo, ref):
    return hub.pkgtools.fetch.get_page(f"https://api.github.com/repos/{github_user}/{github_repo}/git/refs/{ref}",
                                       is_json=True)


def fetch_tag(hub, github_user, github_repo, name):
    return fetch_ref(hub, github_user, github_repo, f"tags/{name}")


class AssetNotFoundError(Exception):
    pass


async def release_gen(hub, github_user, github_repo, release_data=None, tarball=None, assets: dict = None, select=None,
                      filter=None, matcher=None, transform=None, version=None, include=None,
                      sort: SortMethod = SortMethod.VERSION, **kwargs):
    """
	This method will query the GitHub API for releases for a specific project, find the most recent
	release, and then return a dictionary containing the keys "version", "artifacts" and "sha", which
	map to the latest non-prerelease version of the release, a list of an artifact associated with
	this release, and the SHA1 for the commit for this tagged release. This info can easily be added
	to the pkginfo dict.

	There are **three** ways to specify assets you want to grab from a release as Artifacts. One
	is to not specify tarball= or assets=, in which case we attempt to grab the source code by looking
	for the tag associated with the release, and we then grab a tarball associated with that particular
	tag.

	The second way is to specify a string using ``tarball=``. We will look for an asset with this name.
	We call this "tarball" because most of the time, we are grabbing a tarball asset, but this is really
	"Funtoo slang" and the correct GitHub term is "asset". When a match is found, the returned dictionary
	will contain an ``artifacts`` key with a value being a single-element list containing the artifact.

	The third way is to specify a dictionary of key/value pairs, where values are filenames, as a
	``assets=`` keyword argument. We will look for release with assets that contain all specified
	assets. The returned dictionary will contain an ``artifacts`` dictionary containing artifacts,
	indexed by the same keys.

	When using the ``tarball=`` or ``assets=`` method, special python-style string expansions can
	occur in the string. ``{version}``, ``{github_user}``, and ``{github_repo}`` will be replaced
	with their respective ``release_gen()`` arguments. You can also use ``{key}`` to refer to the
	dictionary key if using ``assets=``, and ``{tag}`` to refer to the GitHub tag name associated
	with the release. Additionally, any additional keyword arguments passed to this function
	(which are handled by the wildcard ``**kwargs`` in the function definition) can be expanded,
	allowing this variable-expansion functionality to be extended. Note that these special strings
	are Python-style with single curly-quotes, not Jinja-style with double curly-quotes.

	``release_data`` may contain the full decoded JSON of a query to the /releases endpoint, as returned
	by ``hub.pkgtools.fetch.get_page(url, is_json=True).`` Otherwise, this information will be queried
	directly from GitHub.

	``select`` may contain a regex string which specifies a pattern that must match the tag_name for
	it to be considered.

	``filter`` can be either a regex string or a list of regex strings. Anything that matches
	this string or strings will be excluded.

	``transform`` is a lambda/single-argument function that if specified will be used to
	arbitrarily modify the tag before it is searched for versions, or for the ``select``
	regex.

	``version`` may contain a version string we are looking for specifically. We currently look in the
	tag_name of the release.

	``include`` may contain a list or set of strings (currently supporting "prerelease" and "draft") which
	if defined will be considered as a match. By default, pre-releases and drafts are skipped.
	"""

    if tarball and assets:
        raise ValueError("Please specify tarball= or assets=, but not both.")

    skip_filters = factor_filters(include)

    if not release_data:
        release_data = await fetch_release_data(hub, github_user, github_repo)

    versions_and_release_elements = []

    if matcher is None:
        # Technically, we are scanning tags, and yet many projects use wacky names for their release tags.
        # Therefore, this will use the less strict ReleaseRegexMatcher.
        matcher = ReleaseRegexMatcher()

    for release in release_data:
        if any(release[skip] for skip in skip_filters):
            continue
        match = matcher.match(release['tag_name'], select=select, filter=filter, transform=transform)
        if match:
            if version is not None and match != version:
                continue
            else:
                found_version = match
        else:
            continue
        versions_and_release_elements.append((found_version, release))


    if not len(versions_and_release_elements):
        raise ValueError(f"Could not find a suitable github release for {github_user}/{github_repo}.")

    # By default, we should sort our releases by version, and start with the most recent version. This is important for some GitHub
    # repos that have multiple 'channels' so the releases may vary and most recent by date may not be what we want.

    if sort == SortMethod.VERSION:
        # Have most recent by version at the beginning:
        versions_and_release_elements = sorted(versions_and_release_elements, key=lambda v: matcher.sortable(v[0]),
                                               reverse=True)

    # Iterate, starting with most recent version. We will break from this loop if we are successful. Otherwise, we will
    # keep trying the second-most-recent version, etc. This helps us deal with situations where not all assets are available
    # (see FL-12418)
    while True:
        try:
            if not len(versions_and_release_elements):
                raise IndexError("No more GitHub releases available.")
            upstream_assets_by_name = {}
            version, release = versions_and_release_elements[0]
            versions_and_release_elements = versions_and_release_elements[1:]
            tag_name = release['tag_name']
            for asset in release['assets']:
                upstream_assets_by_name[asset['name']] = asset

            if tarball or assets:

                # We will look for a single specified tarball, or use an assets dict to look for a collection of assets. GitHub calls
                # these things "assets" officially. "Tarball" is sort of funtoo slang for a single asset. It doesn't need to actually
                # be a tarball, it just usually is.

                # Make a singleton dictionary so we can use the same code as the assets processing code. We will convert it back
                # after.

                if tarball:
                    assets = [tarball]

                artifacts = []
                crates_dict = {}

                if isinstance(assets, dict):
                    artifacts = defaultdict(list)
                    for asset_key, asset_filenames in assets.items():
                        if not isinstance(asset_filenames, list):
                            asset_filenames = [asset_filenames] # handle bare string
                        for asset_filename in asset_filenames:
                            # expand {version}, etc.
                            expanded_asset = asset_filename.format(version=version, key=asset_key, github_user=github_user, github_repo=github_repo,tag=tag_name, **kwargs)
                            if expanded_asset in upstream_assets_by_name:
                                upstream_asset = upstream_assets_by_name[expanded_asset]
                                artifact = hub.Artifact(url=upstream_asset['browser_download_url'], final_name=expanded_asset)
                                artifacts[asset_key].append(artifact)
                                if artifact.final_name == "Cargo.lock":
                                    await artifact.fetch()
                                    crates_dict = await hub.pkgtools.rust.get_crates_artifacts(artifact.final_path)
                            elif asset_filename == '<source.tar.gz>':
                                url = f"https://github.com/{github_user}/{github_repo}/archive/refs/tags/{tag_name}.tar.gz"
                                artifacts[asset_key] = hub.pkgtools.ebuild.Artifact(url=url, final_name=f'{github_repo}-{version}.tar.gz')
                            else:
                                hub.pkgtools.model.log.warning(f"Upstream GitHub asset {expanded_asset} not found for release {version}, trying next-most-recent-version...")
                                raise AssetNotFoundError()
                elif isinstance(assets, list):
                    # assets is list:
                    for asset_filename in assets:
                        expanded_asset = asset_filename.format(version=version, github_user=github_user, github_repo=github_repo, tag=tag_name, **kwargs)
                        if expanded_asset in upstream_assets_by_name:
                            upstream_asset = upstream_assets_by_name[expanded_asset]
                            artifact = hub.Artifact(url=upstream_asset['browser_download_url'], final_name=expanded_asset)
                            artifacts.append(artifact)
                            if artifact.final_name == "Cargo.lock":
                                await artifact.fetch()
                                crates_dict = await hub.pkgtools.rust.get_crates_artifacts(artifact.final_path)
                        elif asset_filename == '<source.tar.gz>':
                            url = f"https://github.com/{github_user}/{github_repo}/archive/refs/tags/{tag_name}.tar.gz"
                            artifacts.append(hub.pkgtools.ebuild.Artifact(url=url, final_name=f'{github_repo}-{version}.tar.gz'))
                        else:
                            hub.pkgtools.model.log.warning(f"Upstream GitHub asset {expanded_asset} not found for release {version}, trying next-most-recent-version...")
                            raise AssetNotFoundError()
                else:
                    raise ValueError(f"Unrecognized GitHub release_gen assets= type of {type(assets)}")

                out_dict = {
                    "version": version,
                    "artifacts": artifacts,
                    "tag": tag_name,
                }

                if crates_dict:
                    out_dict.update(crates_dict)

                # Since we want the most recent release, we only process the first
                return out_dict

            else:

                # We want to grab the default tarball for the associated tag:

                desired_tag = tag_name
                hub.pkgtools.model.log.debug(f"github:release_gen: selected release version: {version}, desired tag {desired_tag}")
                tag_data = await fetch_tag(hub, github_user, github_repo, desired_tag)
                if tag_data is None:
                    raise ValueError(
                        f"github:release_gen: Could not retrieve SHA1 for tag {desired_tag} in github repo {github_user}/{github_repo}.")
                sha = tag_data["object"]["sha"]

                ########################################################################################################
                # GitHub does not list this URL in the release's assets list, but it is always available if there is an
                # associated tag for the release. Rather than use the tag name (which would give us a non-distinct file
                # name), we use the sha1 to grab a specific URL and use a specific final name on disk for the artifact.
                ########################################################################################################

                url = f"https://github.com/{github_user}/{github_repo}/tarball/{sha}"
                return {
                    "version": version,
                    "artifacts": [
                        hub.pkgtools.ebuild.Artifact(url=url, final_name=f'{github_repo}-{version}-{sha[:7]}.tar.gz')],
                    "sha": sha,
                    "tag": desired_tag,
                }
        except AssetNotFoundError:
            continue


async def iter_tags_pages(hub, github_user, github_repo):
    page = 1
    while True:
        current_page = await hub.pkgtools.fetch.get_page(
            f"https://api.github.com/repos/{github_user}/{github_repo}/tags?per_page=100&page={page}", is_json=True)
        if not len(current_page):
            break
        yield current_page
        page += 1


async def iter_all_tags(hub, github_user, github_repo):
    async for tags_page in iter_tags_pages(hub, github_user, github_repo):
        for tag_data in tags_page:
            yield tag_data


async def latest_tag_version(hub, github_user, github_repo, tag_data=None, transform=None, select=None, filter=None,
                             matcher=None, version=None):
    """
	This method will look at all the tags in a repository, look for a version string in each tag,
	find the most recent version, and return the version and entire tag data as a tuple.

	``select`` specifies a regex string that must match for the tag version to be considered.

	``version``, if specified, is a version we want. Since we only want this version, we will
	ignore all versions unless we find this specific version.

	If no matching versions, None is returned.
	"""
    if matcher is None:
        matcher = TagRegexMatcher(select=select, filter=filter)
    if tag_data is None:
        tag_data = iter_all_tags(hub, github_user, github_repo)
    versions_and_tag_elements = []
    async for v_tagel in iter_tag_versions(tag_data, select=select, filter=filter, matcher=matcher, transform=transform,
                                           version=version):
        versions_and_tag_elements.append(v_tagel)
    if not len(versions_and_tag_elements):
        return
    else:
        return max(versions_and_tag_elements, key=lambda v: matcher.sortable(v[0]))


async def tag_gen(hub, github_user, github_repo, tag_data=None, select=None, filter=None, matcher=None, transform=None,
                  version=None, **kwargs):
    """
	Similar to ``release_gen``, this will query the GitHub API for the latest tagged version of a project,
	and return a dictionary that can be added to pkginfo containing the version, artifacts and commit sha.

	This method may return None if no suitable tags are found.
	"""
    if matcher is None:
        matcher = TagRegexMatcher(select=select, filter=filter)
    result = await latest_tag_version(hub, github_user, github_repo, tag_data=tag_data, transform=transform,
                                      select=select, filter=filter, matcher=matcher, version=version)
    if result is None:
        return None
    version, tag_data = result
    sha = tag_data['commit']['sha']
    url = f"https://github.com/{github_user}/{github_repo}/tarball/{sha}"
    return {
        "version": version,
        "artifacts": [hub.pkgtools.ebuild.Artifact(url=url, final_name=f'{github_repo}-{version}-{sha[:7]}.tar.gz')],
        "sha": sha,
        "tag": tag_data["name"],
    }
