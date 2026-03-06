import torch
import torch.nn as nn
from typing import Tuple

class TransformerModel(nn.Module):
    def __init__(self, d_model: int = 64, nhead: int = 8, num_layers: int = 2, output_size: int = 10):
        super(TransformerModel, self).__init__()
        # Use a simpler transformer if needed, but this should work.
        self.encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, batch_first=True)
        self.transformer_encoder = nn.TransformerEncoder(self.encoder_layer, num_layers=num_layers)
        self.fc = nn.Linear(d_model, output_size)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x shape: (batch, seq_len, d_model)
        x = self.transformer_encoder(x)
        # Use a way to get the last element that is more likely to be traced correctly
        last_step = x[:, -1, :]
        out = self.fc(last_step)
        return out

def get_model() -> TransformerModel:
    return TransformerModel()

def get_input_shape() -> Tuple[int, ...]:
    return (16, 64) # (seq_len, d_model)
