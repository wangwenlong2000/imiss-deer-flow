from pathlib import Path

def convert_thuocl_to_user_dict(
    thuocl_path: str,
    output_path: str,
    min_len: int = 2,
    min_df: int = 1,
):
    terms = set()

    with open(thuocl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split("\t")
            word = parts[0].strip()
            df = 1

            if len(parts) > 1:
                try:
                    df = int(parts[1])
                except ValueError:
                    df = 1

            if len(word) >= min_len and df >= min_df:
                terms.add(word)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for term in sorted(terms):
            f.write(term + "\n")


if __name__ == "__main__":

    convert_thuocl_to_user_dict(
        thuocl_path="/home/gaozhuohui/LegalRAG/data/lexicon/THUOCL_law.txt",
        output_path="/home/gaozhuohui/LegalRAG/data/lexicon/thuocl_law_user_dict.txt",
    )