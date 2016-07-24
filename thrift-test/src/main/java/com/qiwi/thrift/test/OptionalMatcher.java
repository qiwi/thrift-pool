package com.qiwi.thrift.test;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Optional;

public class OptionalMatcher<T> extends BaseMatcher<Optional<T>> {
    private final Matcher<T> matcher;

    public OptionalMatcher(Matcher<T> matcher) {
        this.matcher = matcher;
    }

    @Override
    public boolean matches(Object o) {
        return o instanceof Optional && matcher.matches(((Optional) o).orElse(null));
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Optional(").appendDescriptionOf(matcher).appendText(")");
    }
}
