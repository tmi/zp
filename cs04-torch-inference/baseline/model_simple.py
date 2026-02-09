import torch
import torch.nn as nn
from typing import Tuple

class SimpleModel(nn.Module):
    def __init__(self, input_size: int = 128, hidden_size: int = 64, output_size: int = 10):
        super(SimpleModel, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, hidden_size)
        self.fc3 = nn.Linear(hidden_size, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.relu(self.fc1(x))
        x = self.relu(self.fc2(x))
        x = self.fc3(x)
        return x

def get_model() -> SimpleModel:
    return SimpleModel()

def get_input_shape() -> Tuple[int, ...]:
    return (128,)
