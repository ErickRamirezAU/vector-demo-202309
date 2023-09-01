

  full_chunk = f"{row.context} {row.title}"
  embedding = openai.Embedding.create(input=full_chunk, model=model_id)['data'][0]['embedding']

  query = SimpleStatement(
                f"""
                INSERT INTO squad
                (id, title, context, question, answers, title_context_embedding)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            )
  
