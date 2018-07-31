/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.csvvalidator;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import uk.gov.nationalarchives.csv.validator.api.java.CsvValidator;
import uk.gov.nationalarchives.csv.validator.api.java.FailMessage;
import uk.gov.nationalarchives.csv.validator.api.java.Substitution;
import uk.gov.nationalarchives.csv.validator.api.java.WarningMessage;

/**
 *
 * @author indika
 */
public class TestCsvValidator {

    public static void main(String[] args) {
        try {
            Boolean failFast = false;
            List pathSubstitutions = new ArrayList();

            Reader csvReader = new FileReader(new File("/workspace/eBTC/eCP_Test_Files/ebPC/CSV/pricelist.csv"));
            Reader csvSchema = new FileReader(new File("/workspace/eBTC/eCP_Test_Files/ebPC/CSV/pricelist.csvs"));

            List<FailMessage> messages = CsvValidator.validate(
                csvReader,
                csvSchema,
                failFast,
                pathSubstitutions, true, false);

            if (messages.isEmpty()) {
                System.out.println("All worked OK");
            } else {
                for (FailMessage message : messages) {
                    if (message instanceof WarningMessage) {
                        System.out.println("Warning: " + message.getMessage());
                    } else {
                        System.out.println("Error: " + message.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
