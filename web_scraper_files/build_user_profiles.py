from typing import Dict, List
import numpy as np
from db_conn import get_db_conn  # χρησιμοποιώ την ίδια σύνδεση με register_vector

# βάρη για τα interaction types
INTERACTION_WEIGHTS: Dict[str, float] = {
    "click": 1.0,
    "like": 2.0,
    "share": 3.0,
    "dislike": -2.0,
}


def fetch_user_ids_with_interactions(conn) -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT user_id
            FROM interactions
            """
        )
        return [row[0] for row in cur.fetchall()]


def fetch_user_interactions_with_embeddings(conn, user_id: int):
    """
    Επιστρέφει λίστα από (embedding, weight) για έναν χρήστη.
    Κάνει join interactions -> articles για να πάρει τα article embeddings.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.embedding, i.interaction_type
            FROM interactions AS i
            JOIN articles AS a ON a.id = i.article_id
            WHERE i.user_id = %s
              AND a.embedding IS NOT NULL;
            """,
            (user_id,),
        )
        rows = cur.fetchall()

    vectors: List[np.ndarray] = []
    weights: List[float] = []

    for emb, interaction_type in rows:
        # emb έρχεται ως list[float] από τον pgvector adapter → το κάνουμε np.array
        if emb is None:
            continue

        w = INTERACTION_WEIGHTS.get(interaction_type, 0.0)
        if w == 0.0:
            continue  # αγνοώ άγνωστους τύπους

        vectors.append(np.array(emb, dtype=np.float32))
        weights.append(w)

    return vectors, weights


def compute_user_embedding(vectors: List[np.ndarray], weights: List[float]) -> np.ndarray | None:
    """
    Υπολογίζει τον σταθμισμένο μέσο όρο των article embeddings για έναν χρήστη.
    Αν δεν υπάρχουν έγκυρα vectors/weights, επιστρέφει None.
    """
    if not vectors:
        return None

    w = np.array(weights, dtype=np.float32)
    V = np.stack(vectors, axis=0)  # shape (N, 384)

    # weighted sum
    weighted_sum = (V * w[:, None]).sum(axis=0)
    total_weight = np.abs(w).sum()

    if total_weight == 0:
        return None

    user_vec = weighted_sum / total_weight

    # optional: L2-normalize για συμβατότητα με cosine similarity
    norm = np.linalg.norm(user_vec)
    if norm > 0:
        user_vec = user_vec / norm

    return user_vec


def save_user_embedding(conn, user_id: int, embedding: np.ndarray | None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE users
            SET embedding = %s
            WHERE id = %s;
            """,
            (embedding.tolist() if embedding is not None else None, user_id),
        )
    conn.commit()


if __name__ == "__main__":
    conn = get_db_conn()

    user_ids = fetch_user_ids_with_interactions(conn)
    print(f"Found {len(user_ids)} users with interactions")

    for uid in user_ids:
        vectors, weights = fetch_user_interactions_with_embeddings(conn, uid)
        user_emb = compute_user_embedding(vectors, weights)

        if user_emb is None:
            print(f"User {uid}: no valid interactions/embeddings, skipping")
            continue

        save_user_embedding(conn, uid, user_emb)
        print(f"User {uid}: embedding updated")
