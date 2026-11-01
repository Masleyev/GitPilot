package com.gh4a.utils;

import com.gh4a.model.Trend;

import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;

public class TranslatorUtils {
    private static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    public static String getCached(String text) {
        if (text == null) {
            return null;
        }
        return CACHE.get(text);
    }

    public static Single<String> translateToRussian(String text) {
        return Single.fromCallable(() -> performTranslation(text, "ru"))
                .subscribeOn(Schedulers.io());
    }

    public static Single<List<Trend>> translateTrends(List<Trend> trends) {
        return Single.fromCallable(() -> {
            if (trends == null || trends.isEmpty()) {
                return trends;
            }

            List<String> textsToTranslate = new ArrayList<>();
            for (Trend t : trends) {
                String desc = t.getDescription();
                if (desc != null && !desc.trim().isEmpty() && !CACHE.containsKey(desc)) {
                    textsToTranslate.add(desc);
                }
            }

            if (!textsToTranslate.isEmpty()) {
                try {
                    StringBuilder batch = new StringBuilder();
                    for (int j = 0; j < textsToTranslate.size(); j++) {
                        if (j > 0) {
                            batch.append("\n~~~~~\n");
                        }
                        batch.append(textsToTranslate.get(j));
                    }

                    String translatedBatch = performTranslation(batch.toString(), "ru");
                    String[] parts = translatedBatch.split("\\s*~~~~~\\s*");

                    if (parts.length == textsToTranslate.size()) {
                        for (int k = 0; k < parts.length; k++) {
                            CACHE.put(textsToTranslate.get(k), parts[k].trim());
                        }
                    } else {
                        for (int k = 0; k < Math.min(parts.length, textsToTranslate.size()); k++) {
                            CACHE.put(textsToTranslate.get(k), parts[k].trim());
                        }
                    }
                } catch (Exception e) {
                    // Ignore batch translation error, fallback to individual or original
                }
            }

            List<Trend> result = new ArrayList<>(trends.size());
            for (Trend t : trends) {
                String desc = t.getDescription();
                if (desc != null) {
                    String cached = CACHE.get(desc);
                    if (cached != null) {
                        result.add(t.withDescription(cached));
                        continue;
                    }
                }
                result.add(t);
            }
            return result;
        }).subscribeOn(Schedulers.io());
    }

    private static String performTranslation(String text, String targetLang) throws Exception {
        if (text == null || text.trim().isEmpty()) {
            return text;
        }

        String cached = CACHE.get(text);
        if (cached != null) {
            return cached;
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
