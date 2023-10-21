package com.example.bigdata;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Scanner;

public class PassengerParser {
    public static void main(String[] args) throws Exception {
        // This is for local testing
        File file = new File("./data-part.csv");
        Scanner rd = new Scanner(file);

        while (rd.hasNextLine()) {
            String line = rd.nextLine();
            PassengerData res = PassengerParser.parseLine(line);
            if (res != null) {
                javafx.util.Pair<String, Integer> kv = res.toKeyValue();
                System.out.println("key:" + kv.getKey() + " val:" + kv.getValue().toString());
            }
        }
    }

    public static PassengerData parseLine(String line) throws ParseException {
        int CASH_PAYMENT_METHOD = 2;

        int idx = 0;
        PassengerData data = new PassengerData();
        int paymentMethod = 0;

        for (String word : line
                .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
            /*
                [1] - Entering Cab Date
                [3] - Passenger Count
                [7] - Entering Zone
                [9] - Payment Method
             */

            switch (idx) {
                case 1:
                    // 2018-12-12 15:13:24
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
                    data.EnterDate = formatter.parse(word);
                    break;

                case 3:
                    data.Count = Integer.parseInt(word);
                    break;

                case 7:
                    data.Zone = Integer.parseInt(word);
                    break;

                case 9:
                    paymentMethod = Integer.parseInt(word);
                    break;

                default:
                    break;
            }

            idx++;
        }

        if (paymentMethod == CASH_PAYMENT_METHOD) {
            return data;
        } else {
            return null;
        }
    }
}
