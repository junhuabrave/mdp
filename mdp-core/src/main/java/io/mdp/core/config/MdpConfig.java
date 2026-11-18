package io.mdp.core.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import io.mdp.core.api.FeedType;

/**
 * Typed view over a properties file. System properties with the same key override file values,
 * so any setting can be changed on the command line ({@code -Dmdp.idle.receiver=yield}).
 */
public final class MdpConfig
{
    private final Properties properties;

    public MdpConfig(final Properties properties)
    {
        this.properties = properties;
    }

    public static MdpConfig load(final Path path) throws IOException
    {
        final Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path))
        {
            properties.load(in);
        }
        return new MdpConfig(properties);
    }

    public static MdpConfig of(final String... keyValues)
    {
        final Properties properties = new Properties();
        for (int i = 0; i < keyValues.length; i += 2)
        {
            properties.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return new MdpConfig(properties);
    }

    public MdpConfig with(final String key, final String value)
    {
        final Properties copy = new Properties();
        copy.putAll(properties);
        copy.setProperty(key, value);
        return new MdpConfig(copy);
    }

    public String get(final String key)
    {
        final String sys = System.getProperty(key);
        if (null != sys)
        {
            return sys.trim();
        }
        final String value = properties.getProperty(key);
        return null == value ? null : value.trim();
    }

    public String get(final String key, final String defaultValue)
    {
        final String value = get(key);
        return null == value || value.isEmpty() ? defaultValue : value;
    }

    public String require(final String key)
    {
        final String value = get(key);
        if (null == value || value.isEmpty())
        {
            throw new IllegalArgumentException("missing required config: " + key);
        }
        return value;
    }

    public int getInt(final String key, final int defaultValue)
    {
        final String value = get(key);
        return null == value || value.isEmpty() ? defaultValue : Integer.parseInt(value);
    }

    public long getLong(final String key, final long defaultValue)
    {
        final String value = get(key);
        return null == value || value.isEmpty() ? defaultValue : Long.parseLong(value);
    }

    public boolean getBoolean(final String key, final boolean defaultValue)
    {
        final String value = get(key);
        return null == value || value.isEmpty() ? defaultValue : Boolean.parseBoolean(value);
    }

    public List<String> getList(final String key)
    {
        final List<String> result = new ArrayList<>();
        final String value = get(key);
        if (null != value)
        {
            for (final String part : value.split(","))
            {
                if (!part.isBlank())
                {
                    result.add(part.trim());
                }
            }
        }
        return result;
    }

    // ---------------------------------------------------------------- well-known settings

    public String mode()
    {
        return get("mdp.mode", "all-in-one");
    }

    public String protocol()
    {
        return require("mdp.protocol");
    }

    public List<Integer> channels()
    {
        final List<Integer> ids = new ArrayList<>();
        for (final String s : getList("mdp.channels"))
        {
            ids.add(Integer.parseInt(s));
        }
        return ids;
    }

    /** Line names for a feed of a channel, for example [A, B]. Defaults to A and B if configured. */
    public List<String> lines(final int channelId, final FeedType feed)
    {
        final String prefix = channelPrefix(channelId) + feed.key();
        final List<String> explicit = getList(prefix + ".lines");
        if (!explicit.isEmpty())
        {
            return explicit;
        }
        final List<String> lines = new ArrayList<>();
        for (final String candidate : new String[]{ "A", "B", "C", "D" })
        {
            if (null != get(prefix + "." + candidate))
            {
                lines.add(candidate);
            }
        }
        return lines;
    }

    public String lineUri(final int channelId, final FeedType feed, final String line)
    {
        return require(channelPrefix(channelId) + feed.key() + "." + line);
    }

    public int lineStreamId(final int channelId, final FeedType feed, final String line, final int defaultValue)
    {
        return getInt(channelPrefix(channelId) + feed.key() + "." + line + ".streamId", defaultValue);
    }

    public String channelSetting(final int channelId, final String name, final String defaultValue)
    {
        return get(channelPrefix(channelId) + name, get("mdp." + name, defaultValue));
    }

    public static String channelPrefix(final int channelId)
    {
        return "mdp.channel." + channelId + ".";
    }
}
