package com.gh4a.utils;

import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;

public class TranslatorUtils {

    public static Single<String> translateToRussian(String text) {
        return Single.fromCallable(() -> performTranslation(text, "ru"))
                .subscribeOn(Schedulers.io());
    }

    private static String performTranslation(String text, String targetLang) throws Exception {
        if (text == null || text.trim().isEmpty()) {
            return text;
        }

        URL url = new URL("https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=" + targetLang + "&dt=t");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(15000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
        conn.setRequestProperty("User-Agent", "Mozilla/5.0 (Android; Mobile; rv:109.0) Gecko/119.0 Firefox/119.0");

        String postData = "q=" + URLEncoder.encode(text, "UTF-8");
        try (OutputStream os = conn.getOutputStream()) {
            os.write(postData.getBytes(StandardCharsets.UTF_8));
            os.flush();
        }

        int code = conn.getResponseCode();
        if (code != 200) {
            throw new RuntimeException("Translation request failed with HTTP " + code);
        }

        StringBuilder response = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }

        JSONArray root = new JSONArray(response.toString());
        JSONArray sentences = root.getJSONArray(0);
        StringBuilder translated = new StringBuilder();
        for (int i = 0; i < sentences.length(); i++) {
            JSONArray sentence = sentences.getJSONArray(i);
            if (!sentence.isNull(0)) {
                translated.append(sentence.getString(0));
            }
        }

        return translated.toString();
    }
}
