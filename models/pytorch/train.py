import torch
import torch.nn as nn
import torch.optim as optim
from model import FraudModel

def train():
    model = FraudModel(input_dim=10)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    X = torch.randn(100, 10)
    y = torch.randint(0, 2, (100, 1)).float()

    for epoch in range(10):
        optimizer.zero_grad()
        outputs = model(X)
        loss = criterion(outputs, y)
        loss.backward()
        optimizer.step()
        print(f"Epoch {epoch+1}, Loss {loss.item()}")

    torch.save(model, "../saved_models/fraud.pt")

if __name__ == "__main__":
    train()
