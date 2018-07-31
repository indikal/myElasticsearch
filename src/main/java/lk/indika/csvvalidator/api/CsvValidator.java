package lk.indika.csvvalidator.api;

/**
 *  CsvValidator validates any delimited file against its spec
 */
public interface CsvValidator {

    /**
     * isValid() or getValidationDetails() can be executed in any sequence.
     */
    
    /**
     * @return  true if valid else false
     */
    boolean isValid();

    /**
     * You want to call this only when isValid returns false and this can be used to 
     *  convey error report back to the third party
     * @return Detail Report of all failures
     */
    String getValidationDetails();
}
