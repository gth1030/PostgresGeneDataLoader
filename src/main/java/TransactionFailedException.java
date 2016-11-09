/**
 * Created by kitae on 8/29/16.
 * Custom exception for efficient control of an error.
 */
class TransactionFailedException extends Exception {
    TransactionFailedException (String message) {
        super(message);
    }
}
