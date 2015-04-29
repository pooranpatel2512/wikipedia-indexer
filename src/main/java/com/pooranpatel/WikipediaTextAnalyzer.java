package com.pooranpatel;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.TypeTokenFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer;
import java.util.*;

public class WikipediaTextAnalyzer extends Analyzer {

    public static final Set<String> STOP_TYPES = new HashSet<>();

    static {
        STOP_TYPES.add("<EMAIL>");
        STOP_TYPES.add("<SOUTHEAST_ASIAN>");
        STOP_TYPES.add("<IDEOGRAPHIC>");
        STOP_TYPES.add("<HIRAGANA>");
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer t = new UAX29URLEmailTokenizer();
        TokenStream tok = new StandardFilter(t);
        tok = new LowerCaseFilter(tok);
        tok = new StopFilter(tok, StandardAnalyzer.STOP_WORDS_SET);
        tok = new TypeTokenFilter(tok, STOP_TYPES);
        return new TokenStreamComponents(t, tok);
    }
}
