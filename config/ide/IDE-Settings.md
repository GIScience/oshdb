# Setting up your IDE
This link-list explains how to set up your IDE to use the designated code-style. For more information see the general [guidelines](https://confluence.gistools.geog.uni-heidelberg.de/display/oshdb/Contributing)!


Unfortunately the [EditorConfig](http://editorconfig.org)-Project is not yet sophisticated enough. So here is an explanation of each major IDE. The files are taken from [this repository](https://github.com/google/styleguide) and should be updated once in a while! Note that this only guarantees the right formatting for .java-code. See the codestyle-specifications for other languages.

This [plugin](https://maven.apache.org/plugins/maven-checkstyle-plugin/) may help you analyse your formatting.

## IntelliJ

Please follow this [guide](https://www.jetbrains.com/help/idea/configuring-code-style.html#d80998e33) using this [intellij-java-google-style.xml](/config/ide/intellij-java-google-style.xml).

## Eclipse

Please use this [guide](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Fcodestyle%2Fref-preferences-formatter.htm) to import the dedicated [eclipse-java-google-style.xml](/config/ide/eclipse-java-google-style.xml).

## NetBeans

To our knowledge NetBeans uses [this](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html)[codestyle](https://netbeans.org/community/guidelines/code-conventions.html) and is hardly convinced of doing differently.

If you are using an older version (<8.1), you might find this [plugin](http://plugins.netbeans.org/plugin/3413/checkstyle-beans) interesting.

On the other hand, this [plugin](https://github.com/markiewb/eclipsecodeformatter_for_netbeans) can do the job also using the dedicated [eclipse-java-google-style.xml](/config/ide/eclipse-java-google-style.xml).
