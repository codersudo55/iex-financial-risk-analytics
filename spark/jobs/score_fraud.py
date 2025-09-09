import torch

def score_transactions(transactions, model_path="models/saved_models/fraud.pt"):
    model = torch.load(model_path)
    model.eval()
    with torch.no_grad():
        scores = model(transactions)
    return scores
