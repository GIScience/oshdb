Here are some suggestions for how to set up your IDE and code linting tools to use the OSHDB code style.

# IDE Settings

## IntelliJ

Please follow this [guide](https://www.jetbrains.com/help/idea/configuring-code-style.html#d80998e33) using this [intellij-java-google-style.xml](/config/ide/intellij-java-google-style.xml).

## Eclipse

Please use this [guide](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Fcodestyle%2Fref-preferences-formatter.htm) to import the dedicated [eclipse-java-google-style.xml](/config/ide/eclipse-java-google-style.xml).

# Checkstyle

[Checkstyle](http://checkstyle.sourceforge.net/) can check if Java code complies to a given set of code style rules. It can be used standalone, but there are also plugins for IDEs like [intelliJ](https://plugins.jetbrains.com/plugin/1065-checkstyle-idea) and [eclipse](http://checkstyle.org/eclipse-cs/), or for build tools like [maven](https://maven.apache.org/plugins/maven-checkstyle-plugin/).

For checkstyle, use our dedicated [checkstyle-google-ohsome.xml](https://gitlab.gistools.geog.uni-heidelberg.de/giscience/big-data/ohsome/parent/-/blob/master/ohsome-codestyle/src/main/resources/checkstyle-google-ohsome.xml) configuration file.
