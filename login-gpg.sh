#!/bin/bash
gpg -o /dev/null --local-user [your key] -as <(echo 1234) && echo "The correct passphrase was entered for this key"
