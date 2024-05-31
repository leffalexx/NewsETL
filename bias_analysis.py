from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

tokenizer = AutoTokenizer.from_pretrained("bucketresearch/politicalBiasBERT")
model = AutoModelForSequenceClassification.from_pretrained(
    "bucketresearch/politicalBiasBERT")


def predict_political_bias(text):
    inputs = tokenizer(text, return_tensors="pt",
                       max_length=512, truncation=True, padding=True)

    outputs = model(**inputs)
    logits = outputs.logits

    predicted_class_idx = torch.argmax(logits, dim=1).item()

    model_classes = ["Left", "Neutral", "Right"]
    predicted_class = model_classes[predicted_class_idx]

    return predicted_class
