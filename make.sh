#!/bin/bash

VERSION=`cat VERSION`
PKGNAME="funtoo-metatools"

prep() {
	install -d dist
	rm -f dist/$PKGNAME-$VERSION*
	cat > metatools/version.py << EOF
__version__ = "$VERSION"
EOF
	for x in setup.py; do
		sed -e "s/##VERSION##/$VERSION/g" \
		${x}.in > ${x}
	done
}

commit() {
	git commit -a -m "$VERSION release."
	git tag -f "$VERSION"
	git push
	git push --tags
	python3 setup.py sdist
}

if [ "$1" = "prep" ]
then
	prep
elif [ "$1" = "commit" ]
then
	commit
elif [ "$1" = "all" ]
then
	prep
	commit
fi
