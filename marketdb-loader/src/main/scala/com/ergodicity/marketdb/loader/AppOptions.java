package com.ergodicity.marketdb.loader;


import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;


public class AppOptions {
    public static final String LOADER_OPT = "loader";
    public static final String FROM_OPT = "from";
    public static final String TO_OPT = "to";

    @SuppressWarnings("AccessStaticViaInstance")
    public static org.apache.commons.cli.Options buildOptions() {
        org.apache.commons.cli.Options options = new org.apache.commons.cli.Options();

        Option loader = OptionBuilder.withArgName(LOADER_OPT)
                .hasArg()
                .withDescription("use given loader")
                .isRequired()
                .create(LOADER_OPT);

        Option from = OptionBuilder.withArgName(FROM_OPT)
                .hasArg()
                .withDescription("load data from given date inclusive (YYYYMMdd)")
                .isRequired()
                .create(FROM_OPT);

        Option to = OptionBuilder.withArgName(TO_OPT)
                .hasArg()
                .withDescription("load data to given date inclusive (YYYYMMdd)")
                .isRequired()
                .create(TO_OPT);

        options.addOption(loader);
        options.addOption(from);
        options.addOption(to);

        return options;
    }
}
