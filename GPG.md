# GPG2

Short cheat sheet for gpg2.

## Prerequisites on OSX

```
brew install gpg2
```

## Check if your password is correct

```
gpg2 -o /dev/null --local-user <KEYID> -as <(echo 1234) && echo "The correct passphrase was entered for this key"
```

If you entered your key password once, the gpg agent stores it. 
So on a second invocation you are no more prompted for your password.

## List keys (with additional short key id)

```
gpg --list-keys --keyid-format short
```

The short id is the part after the slash after the encoding algorithm.
