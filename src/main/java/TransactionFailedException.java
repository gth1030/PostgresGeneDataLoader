/**
 * Created by kitae on 8/29/16.
 * Custom exception for efficient control of an error.
 */
public class TransactionFailedException extends Exception {
    public TransactionFailedException (String message) {
        super(message);
    }
}
