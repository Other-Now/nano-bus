# source me: JDK 21 + Maven. Git Bash on the dev laptop uses the portable copies.
if [ -d /d/self_projects/.tools ]; then
  export JAVA_HOME="D:/self_projects/.tools/jdk-21.0.12.1+1"
  export PATH="/d/self_projects/.tools/jdk-21.0.12.1+1/bin:/d/self_projects/.tools/apache-maven-3.9.11/bin:$PATH"
elif [ -x "$HOME/jdk21/bin/java" ]; then
  export JAVA_HOME="$HOME/jdk21"; export PATH="$JAVA_HOME/bin:$PATH"
fi
