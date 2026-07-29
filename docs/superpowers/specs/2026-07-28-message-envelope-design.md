# Add Message Envelope With Application Properties

Replace the root JSON array contract with a required envelope that carries Azure Service Bus application properties and message bodies separately. This lets operators attach the same custom properties to every message from one input file without putting those properties in each body.

## Quick Path

1. Write each input file as an object with `properties` and `data` keys.
2. Put common Azure application properties in `properties`.
3. Put one JSON object per message in `data`.

```json
{
  "properties": {
    "source": "import-tool",
    "priority": 3,
    "isRetry": false
  },
  "data": [
    {
      "orderId": "A-1001"
    },
    {
      "orderId": "A-1002"
    }
  ]
}
```

`data/orders.json` still targets queue `orders`.

## Contract

| Field | Required | Valid value | Delivery behavior |
| --- | --- | --- | --- |
| Root value | Yes | JSON object | Must contain `properties` and `data`; additional fields are ignored. |
| `properties` | Yes | Object with string keys and primitive JSON values | Passed as `application_properties` on every message created from `data`. |
| `data` | Yes | Array of JSON objects | Each element is compactly serialized into one independent message body. |

Property values may be strings, finite numbers, booleans, or `null`. Nested objects, nested arrays, and non-finite numbers are invalid. Data objects retain the existing recursive finite-number validation.

An empty `properties` object and an empty `data` array are valid. Empty data sends no messages and succeeds.

## Processing Flow

1. Discover the input file and derive its queue name from the file name.
2. Decode the complete UTF-8 document.
3. Validate the required root fields, every application property, and every data object before opening a sender; ignore additional root fields.
4. Open the queue sender only after validation succeeds.
5. Serialize each `data` object as the message body and attach a copy of the validated `properties` mapping as Azure `application_properties`.
6. Send messages using the existing dynamic batching and failure-isolation behavior.

## Failure Behavior

Invalid envelopes, missing keys, invalid property values, or invalid data objects fail that file before any sender is opened. Additional root fields are ignored. Later files continue to be processed. Existing per-file logging, partial-send counts, summary behavior, duplicate risk, and exit codes remain unchanged.

## Scope

This change does not add per-message property overrides, Azure system-property mapping, property serialization, or changes to queue selection, batching, retries, file lifecycle, or authentication.

## Verification Checklist

- [ ] The root JSON value is an object containing `properties` and `data`; additional fields are ignored.
- [ ] Both keys are required and have their expected types.
- [ ] Properties contain only supported primitive JSON values and finite numbers.
- [ ] Each `data` object becomes a message with the same application properties.
- [ ] Invalid files fail before sender creation and do not prevent later files from running.
- [ ] Existing batching, safe logging, partial-send accounting, and exit-code tests still pass.
