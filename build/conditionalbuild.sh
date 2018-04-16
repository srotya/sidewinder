#!/bin/bash -e
# ref: https://raw.githubusercontent.com/cdown/travis-automerge/master/travis-automerge

printf "Master branch will cut a release to Maven central"
mkdir -p "/tmp/secrets"
printf "Extracting SSH Key"
openssl aes-256-cbc -K $encrypted_77485d179e0e_key -iv $encrypted_77485d179e0e_iv -in build/secrets.tar.enc -out /tmp/secrets/secrets.tar -d
tar xf /tmp/secrets/secrets.tar -C /tmp/secrets/

mkdir -p ~/.ssh
chmod 700 ~/.ssh
cp /tmp/secrets/secrets/id_rsa ~/.ssh/id_rsa
chmod 400 ~/.ssh/id_rsa

gpg -q --fast-import /tmp/secrets/secrets/codesign.asc >> /dev/null



if [ ! -z "$TRAVIS_TAG" ]; then
   printf "Don't execute releases on tag builds request"
   exit 0
fi

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
   printf "Don't execute releases on pull request"
   exit 0
fi

if [ "snapshot" == "$TRAVIS_BRANCH" ]; then
     printf "Snapshot branch will deploy snapshot to Maven central"
     mvn -T2 -B clean deploy
fi

if [ "master" == "$TRAVIS_BRANCH" ]; then
    git remote set-url origin $REPO
    git checkout master || git checkout -b master
    git reset --hard origin/master

    git config --global user.name "Travis CI"
    git config --global user.email "$COMMIT_AUTHOR_EMAIL"
    git config --global alias.squash '!f(){ git reset --soft HEAD~${1} && git commit -m"[ci skip] $(git log --format=%B --reverse HEAD..HEAD@{1})"; };f'

    echo "Release Build number:$TRAVIS_BUILD_NUMBER" > build/build-info.txt
    echo "Release Commit number:$TRAVIS_COMMIT" >> build/build-info.txt
    git add build/build-info.txt
    git commit -m "[ci skip] Updating build-info file"
    git push

    git checkout master || git checkout -b master
    git reset --hard origin/master

    mvn -T2 -B -Darguments=-Dgpg.passphrase=$passphrase release:clean release:prepare release:perform --settings settings.xml

	# Trigger Docker Build
	# TAG=$(git describe --abbrev=0 --tags)
	# curl -H "Content-Type: application/json" --data "{\"source_type\": \"Tag\", \"source_name\": \"$TAG\"}" -X POST https://registry.hub.docker.com/u/srotya/sidewinder/trigger/$BUILD_TOKEN/

	#echo "Cleaning up commits"
	#git checkout master || git checkout -b master
    #git reset --hard origin/master
	#git squash 2
    #git push --force
fi
