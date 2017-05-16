# GPG2

Short cheat sheet for gpg2.

## Prerequisites on OSX

```
brew install gpg2
```

## List keys (with additional short key id)

```
gpg --list-keys --keyid-format short
```

The short id is the part after the slash after the encoding algorithm.

## Check if your password is correct

```
gpg2 -o /dev/null --local-user [KEYID] -as <(echo 1234) && echo "The correct passphrase was entered for this key"
```

If you entered your key password once, the gpg agent stores it. 
So on a second invocation you are no more prompted for your password.

## Some links regarding Maven central deployment:

* http://central.sonatype.org/pages/ossrh-guide.html
* http://central.sonatype.org/pages/apache-maven.html
* http://central.sonatype.org/pages/working-with-pgp-signatures.html#generating-a-key-pair
* http://maven.apache.org/guides/mini/guide-encryption.html
* http://central.sonatype.org/pages/releasing-the-deployment.html
* https://oss.sonatype.org/#stagingRepositories