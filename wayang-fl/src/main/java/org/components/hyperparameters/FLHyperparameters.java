package org.components.hyperparameters;

public class FLHyperparameters<T> extends Hyperparameters {
    public FLHyperparameters(int epochs, T zero, T init, float learning_rate) {
        update_server_hyperparams("epochs", epochs);
        update_server_hyperparams("zero", zero);
        update_server_hyperparams("init", init);
        update_server_hyperparams("learning_rate", learning_rate);
    }
}
