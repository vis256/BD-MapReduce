package com.example.bigdata;

import javafx.util.Pair;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class PassengerData {
    public Date EnterDate;
    public int Count;
    public int Zone;

    public PassengerData() { }

    @Override
    public String toString() {
        return "PassengerData{" +
                "EnterDate=" + EnterDate +
                ", Count=" + Count +
                ", Zone=" + Zone +
                '}';
    }

    public static String KEY_DELIMETER = "|";

    public Pair<String, Integer> toKeyValue() {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM", Locale.ENGLISH);
        return new Pair<>(formatter.format(EnterDate) + KEY_DELIMETER + Zone, Count);
    }
}
