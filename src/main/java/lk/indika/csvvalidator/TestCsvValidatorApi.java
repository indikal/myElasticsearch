package lk.indika.csvvalidator;

import java.io.UnsupportedEncodingException;
import lk.indika.csvvalidator.api.CsvValidator;
import lk.indika.csvvalidator.api.CsvValidatorImpl;
import lk.indika.csvvalidator.api.Field;
import lk.indika.csvvalidator.api.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Reference Integration code
 * https://github.com/intesar/lib1.git
 */
public class TestCsvValidatorApi {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        boolean optional = true;
        boolean notOptional = false;

        // List item holds information about each field.
        List<Field> list = new ArrayList<Field>();
        
        // Field represent each item which contains 4 elements 
        // element 1 --> name of field
        // element 2 --> type of the field
        // element 3 --> optional flag
        // element 4 --> regex if any
        
        list.add(new Field(1, "supplier item id", Type.TEXT, notOptional)); //supplieritemid
        list.add(new Field(2, "ean id", Type.TEXT, optional)); //eanid 
        list.add(new Field(3, "manufacturer id", Type.TEXT, optional)); //manufacturerid 
        list.add(new Field(4, "dml type", Type.TEXT, notOptional)); //dmltype
        list.add(new Field(5, "replaces item id", Type.TEXT, optional)); //replacesitemid 
        list.add(new Field(6, "replaced by item id", Type.TEXT, optional)); //replacedbyitemid 
        list.add(new Field(7, "is orderable", Type.NUMBER, notOptional)); //isorderable 
        list.add(new Field(8, "is batch", Type.TEXT, optional)); //is_batch 
        list.add(new Field(9, "not in stock item", Type.TEXT, optional)); //not_in_stock_item 
        list.add(new Field(10, "variable quantity", Type.TEXT, optional)); //variablequantity 
        list.add(new Field(11, "variable amount", Type.TEXT, optional)); //variable_amount 
        list.add(new Field(12, "availability from date", Type.DATE, optional, "yyyy-MM-dd")); //availability_from_date 
        list.add(new Field(13, "availability until date", Type.DATE, optional, "yyyy-MM-dd")); //availability_until_date 
        list.add(new Field(14, "packaging level", Type.TEXT, optional)); //packaging_level 
        list.add(new Field(15, "package type", Type.TEXT, optional)); //package_type 
        list.add(new Field(16, "package mark", Type.TEXT, optional)); //package_mark 
        list.add(new Field(17, "package deposite code", Type.TEXT, optional)); //package_deposite_code 
        list.add(new Field(18, "lead time", Type.TEXT, optional)); //lead_time 
        list.add(new Field(19, "lead time type", Type.TEXT, optional)); //lead_time_type 
        list.add(new Field(20, "commodity category", Type.TEXT, optional)); //commoditycategory 
        list.add(new Field(21, "brand name", Type.TEXT, optional)); //brandname 
        list.add(new Field(22, "denomination", Type.TEXT, notOptional)); //denomination 
        list.add(new Field(23, "description", Type.TEXT, optional)); //description 
        list.add(new Field(24, "vat code", Type.TEXT, optional, "^vat$")); //vatcode 
        list.add(new Field(25, "vat rate", Type.NUMBER, optional)); //vatrate 
        list.add(new Field(26, "price qualifier", Type.TEXT, notOptional)); //price_qualifier
        list.add(new Field(27, "price", Type.TEXT, notOptional, "^(\\d+(?:[\\.\\,]\\d{2})?)$")); //price //regex = ".*(" + regex + ").*";

        // Instantiating Valiator
        CsvValidator validator1;
        try {
            validator1 = new CsvValidatorImpl("/workspace/eBTC/eCP_Test_Files/ebPC/CSV/pricelist.csv", list, ";");

            // checking isValid
            if (!validator1.isValid()) {
                // printing to console if not valid
                // you can print this to email or log
                System.out.println(validator1.getValidationDetails());
            }
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(TestCsvValidatorApi.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
