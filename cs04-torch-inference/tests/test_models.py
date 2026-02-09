import torch
from baseline.model_simple import get_model as get_simple
from baseline.model_rnn import get_model as get_rnn
from baseline.model_transformer import get_model as get_transformer

def test_models():
    simple = get_simple()
    rnn = get_rnn()
    transformer = get_transformer()

    assert isinstance(simple, torch.nn.Module)
    assert isinstance(rnn, torch.nn.Module)
    assert isinstance(transformer, torch.nn.Module)

def test_inference_shapes():
    simple = get_simple()
    x = torch.randn(1, 128)
    out = simple(x)
    assert out.shape == (1, 10)

    rnn = get_rnn()
    x = torch.randn(1, 10, 32)
    out = rnn(x)
    assert out.shape == (1, 10)

    transformer = get_transformer()
    x = torch.randn(1, 16, 64)
    out = transformer(x)
    assert out.shape == (1, 10)
