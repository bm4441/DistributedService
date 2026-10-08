package com.zebra.ai.distributed;

/**
 * The product name of the device -- the system property ro.product.name, which android.os.Build.PRODUCT holds, and
 * which any app may read -- and how the log forwarder adds it to the device status message it passes on. The
 * name tells apart units the model does not: "TC501W" is the Wi-Fi unit and "TC501L" the one with the cellular
 * modem, both with the model "TC501".
 *
 * The status message is made by the service (DeviceStatus.toJson()) and already carries the device id; the
 * forwarder, a process of its own, puts the product name in as the message goes by, so the dashboard gets the
 * two together and the service is not touched:
 *
 *   {"type":"device","source":"DistributedService",...,"deviceId":"b7c9a585bbfb04f1","model":"TC501",...,"product":"TC501W"}
 *
 * Plain Java, no Android: it runs in plain unit tests (the caller passes in what it read from the system).
 */
final class ProductName {

    static final String JSON_KEY = "product";

    private ProductName() {
    }

    /**
     * The status message with "product":"<name>" added as its last member, so everything before it -- "type" first
     * of all -- stays as it was. A name that is missing or blank is sent as null: never guessed. A message that is
     * not a JSON object, or that has a "product" member already, is passed on unchanged.
     */
    static String addTo(String statusJson, String product) {
        if (statusJson == null) {
            return null;
        }
        String text = statusJson.trim();
        if (text.length() < 2 || text.charAt(0) != '{' || text.charAt(text.length() - 1) != '}') {
            return statusJson;
        }
        if (text.contains("\"" + JSON_KEY + "\":")) {
            return statusJson;
        }
        String withoutClosingBrace = text.substring(0, text.length() - 1).trim();
        String value = new JsonText().str(product != null && !product.trim().isEmpty() ? product : null).toString();
        return withoutClosingBrace + (withoutClosingBrace.endsWith("{") ? "" : ",")
                + "\"" + JSON_KEY + "\":" + value + "}";
    }
}
