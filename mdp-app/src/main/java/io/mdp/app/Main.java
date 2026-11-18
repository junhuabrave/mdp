package io.mdp.app;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

import io.mdp.app.bench.LatencyBenchmark;
import io.mdp.app.sim.FeedSimulator;
import io.mdp.core.config.MdpConfig;

/**
 * Launcher.
 * <pre>
 *   mdp &lt;config.properties&gt;              run the role given by mdp.mode
 *   mdp simulate &lt;config.properties&gt;     synthetic exchange feed (with loss injection)
 *   mdp benchmark [options]              latency benchmark
 * </pre>
 * Any setting can be overridden with -Dkey=value.
 */
public final class Main
{
    private Main()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length == 0)
        {
            System.err.println("usage: mdp <config.properties> | simulate <config.properties> | benchmark [args]");
            System.exit(2);
        }

        switch (args[0])
        {
            case "simulate" -> FeedSimulator.main(new String[]{ args[1] });
            case "benchmark" -> LatencyBenchmark.main(java.util.Arrays.copyOfRange(args, 1, args.length));
            default ->
            {
                final MdpConfig config = MdpConfig.load(Path.of(args[0]));
                final CountDownLatch shutdown = new CountDownLatch(1);
                final FeedHandlerApp app = FeedHandlerApp.start(config, new TopOfBookLogger());
                Runtime.getRuntime().addShutdownHook(new Thread(() ->
                {
                    app.close();
                    shutdown.countDown();
                }, "mdp-shutdown"));
                shutdown.await();
            }
        }
    }
}
