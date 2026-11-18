package io.mdp.core.api;

public enum FeedType
{
    INCREMENTAL("incremental"),
    SNAPSHOT("snapshot"),
    DEFINITION("definition");

    private final String key;

    FeedType(final String key)
    {
        this.key = key;
    }

    public String key()
    {
        return key;
    }
}
