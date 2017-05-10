package org.qcri.rheem.basic.operators;

import org.qcri.rheem.basic.data.RecordDinamic;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


/**
 * This source reads a JSON file and outputs is as {@Link RecordDinamic}.
 */
public class JSONSource extends UnarySource<RecordDinamic> {

    private final String       inputUrl;

    private final String       enconding;

    private final List<String> words_tokens = new ArrayList<>();

    public JSONSource(String inputUrl) {
        this(inputUrl, "UTF-8");
    }

    public JSONSource(String inputUrl, String encoding) {
        super(DataSetType.createDefault(RecordDinamic.class));
        this.inputUrl = inputUrl;
        this.enconding = encoding;
    }

    public JSONSource(String inputUrl, String[] words) {
        this(inputUrl, "UTF-8");
        this.words_tokens.addAll(Arrays.asList(words));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JSONSource(JSONSource that) {
        super(that);
        this.words_tokens.addAll(Arrays.asList(that.getWords_tokens()));
        this.inputUrl = that.getInputUrl();
        this.enconding = that.getEnconding();
    }


    public TextFileSource getFile() {
        return new TextFileSource(inputUrl, enconding);
    }

    public String[] getWords_tokens() {
        return words_tokens.toArray(new String[0]);
    }

    public String getInputUrl() {
        return inputUrl;
    }

    public String getEnconding() {
        return enconding;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        return (new TextFileSource(this.inputUrl, this.enconding)).createCardinalityEstimator(outputIndex, configuration);
    }
}
