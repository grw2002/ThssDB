package cn.edu.thssdb.exception;

public class TableOccupiedException extends RuntimeException {

    @Override
    public String getMessage() {
        return "Exception: Table is under use!";
    }
}