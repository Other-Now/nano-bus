#!/bin/bash
# run a harness command from an ext4 work directory: scripts/wsl.sh chaos --mode safe
set -e
here=$(cd "$(dirname "$0")/.." && pwd)
. "$here/env.sh"
[ -x "$HOME/jdk21/bin/java" ] && { export JAVA_HOME="$HOME/jdk21"; export PATH="$JAVA_HOME/bin:$PATH"; }
mkdir -p ~/nanobus-run && cd ~/nanobus-run
cp "$here/target/nano-bus.jar" .
mkdir -p work "$here/results/local"
exec java -Xmx96m -cp nano-bus.jar nanobus.Main "$@" --out "$here/results/local"
