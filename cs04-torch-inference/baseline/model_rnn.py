import torch
import torch.nn as nn
from typing import Tuple

class RNNModel(nn.Module):
    def __init__(self, input_size: int = 32, hidden_size: int = 64, output_size: int = 10, num_layers: int = 2):
        super(RNNModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.rnn = nn.RNN(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x shape: (batch, seq_len, input_size)
        out, _ = self.rnn(x)
        # out shape: (batch, seq_len, hidden_size)
        out = self.fc(out[:, -1, :])
        return out

def get_model() -> RNNModel:
    return RNNModel()

def get_input_shape() -> Tuple[int, ...]:
    return (10, 32) # (seq_len, input_size)
