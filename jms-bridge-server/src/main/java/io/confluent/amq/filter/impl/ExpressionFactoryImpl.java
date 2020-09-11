/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.filter.impl;

import io.confluent.amq.filter.BridgeFilter;
import io.confluent.amq.filter.ExpressionFactory;
import io.confluent.amq.filter.FilterSupport;
import io.confluent.amq.filter.PropertyExtractor;
import java.io.StringReader;
import java.util.Optional;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.filter.impl.FilterImpl;
import org.apache.activemq.artemis.selector.filter.ComparisonExpression;
import org.apache.activemq.artemis.selector.filter.FilterException;
import org.apache.activemq.artemis.selector.filter.Filterable;
import org.apache.activemq.artemis.selector.filter.PropertyExpression;
import org.apache.activemq.artemis.selector.hyphenated.HyphenatedParser;
import org.apache.activemq.artemis.selector.strict.StrictParser;

public final class ExpressionFactoryImpl implements ExpressionFactory {

  private static final String CONVERT_STRING_EXPRESSIONS_PREFIX = "convert_string_expressions:";
  private static final String HYPHENATED_PROPS_PREFIX = "hyphenated_props:";
  private static final String NO_CONVERT_STRING_EXPRESSIONS_PREFIX
      = "no_convert_string_expressions:";
  private static final String NO_HYPHENATED_PROPS_PREFIX = "no_hyphenated_props:";

  private static final ExpressionFactoryImpl INSTANCE = new ExpressionFactoryImpl();

  public static ExpressionFactoryImpl getInstance() {
    return INSTANCE;
  }

  private ExpressionFactoryImpl() {

  }

  public BridgeFilter parseAmqFilter(String sql) {
    try {
      Filter amqFilter = FilterImpl.createFilter(sql);
      return new BridgeFilter() {
        @Override
        public boolean match(FilterSupport filterable) {
          return amqFilter.match(new FilterableFilterSupport(filterable));
        }

        @Override
        public String getFilterString() {
          return amqFilter.getFilterString().toString();
        }
      };
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
  public PropertyExtractor parsePropertyExtractor(String sql) {

    //NOTE: borrowed most of this code from org.apache.activemq.artemis.selector.impl.SelectorParser
    //Thank you to whomever wrote it originally.
    String actual = sql;
    boolean convertStringExpressions = false;
    boolean hyphenatedProps = false;
    while (true) {
      if (actual.startsWith(CONVERT_STRING_EXPRESSIONS_PREFIX)) {
        convertStringExpressions = true;
        actual = actual.substring(CONVERT_STRING_EXPRESSIONS_PREFIX.length());
        continue;
      }
      if (actual.startsWith(HYPHENATED_PROPS_PREFIX)) {
        hyphenatedProps = true;
        actual = actual.substring(HYPHENATED_PROPS_PREFIX.length());
        continue;
      }
      if (actual.startsWith(NO_CONVERT_STRING_EXPRESSIONS_PREFIX)) {
        convertStringExpressions = false;
        actual = actual.substring(NO_CONVERT_STRING_EXPRESSIONS_PREFIX.length());
        continue;
      }
      if (actual.startsWith(NO_HYPHENATED_PROPS_PREFIX)) {
        hyphenatedProps = false;
        actual = actual.substring(NO_HYPHENATED_PROPS_PREFIX.length());
        continue;
      }
      break;
    }

    if (convertStringExpressions) {
      ComparisonExpression.CONVERT_STRING_EXPRESSIONS.set(true);
    }
    try {
      PropertyExpression e = null;
      if (hyphenatedProps) {
        HyphenatedParser parser = new HyphenatedParser(new StringReader(actual));
        e = parser.variable();
      } else {
        StrictParser parser = new StrictParser(new StringReader(actual));
        e = parser.variable();
      }

      final PropertyExpression propExpression = e;
      return (message) -> {
        try {
          return Optional.ofNullable(propExpression.evaluate(new FilterableFilterSupport(message)));
        } catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      };
    } catch (Throwable e) {
      FilterException fe = new FilterException(actual, e);
      throw new RuntimeException(fe);
    } finally {
      if (convertStringExpressions) {
        ComparisonExpression.CONVERT_STRING_EXPRESSIONS.remove();
      }
    }
  }

  static class FilterableFilterSupport implements Filterable {

    final FilterSupport filterSupport;

    FilterableFilterSupport(FilterSupport filterSupport) {
      this.filterSupport = filterSupport;
    }

    @Override
    public <T> T getBodyAs(Class<T> type) throws FilterException {
      return null;
    }

    @Override
    public Object getProperty(SimpleString name) {
      return filterSupport.getProperty(name.toString());
    }

    @Override
    public Object getLocalConnectionId() {
      return null;
    }
  }
}
