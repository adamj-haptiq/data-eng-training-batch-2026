from etl.nasdaq_tickers import sample_tickers


def test_sample_tickers_reproducible():
    symbols = [f"T{i}" for i in range(50)]
    sample1 = sample_tickers(symbols, sample_size=25, seed=42)
    sample2 = sample_tickers(symbols, sample_size=25, seed=42)
    assert sample1 == sample2
    assert len(sample1) == 25


def test_sample_tickers_returns_all_when_small_pool():
    symbols = ["A", "B", "C"]
    result = sample_tickers(symbols, sample_size=25)
    assert result == ["A", "B", "C"]
