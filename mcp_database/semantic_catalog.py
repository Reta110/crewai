"""
Semantic Catalog: El cerebro de negocio del MCP.

Este módulo gestiona el "Catálogo Semántico" - un archivo YAML que mapea
conceptos de negocio a objetos de base de datos. Es LA pieza clave que
permite a la IA entender una BD que nunca ha visto.

El catálogo contiene:
- Glosario de términos de negocio (OC = "Orden de Compra")
- Significado de cada tabla y columna
- Relaciones entre tablas
- Alias/sinónimos (cómo los usuarios llaman a las cosas)
- Cálculos comunes (total OC = SUM(can * pre))
- Ejemplos de preguntas con su SQL

Uso:
    catalog = SemanticCatalog("mcp_database/catalog.yaml")
    context = catalog.get_relevant_context("¿Cuál es el total de la OC 58136?")
    # -> Retorna todo lo que la IA necesita para responder
"""

import os
from typing import Any

import yaml


class SemanticCatalog:
    """Gestiona el catálogo semántico y busca contexto relevante."""

    def __init__(self, catalog_path: str):
        if not os.path.exists(catalog_path):
            raise FileNotFoundError(
                f"Catálogo no encontrado: {catalog_path}\n"
                "Genera uno con: python -m mcp_database.generate_catalog"
            )
        with open(catalog_path, encoding="utf-8") as f:
            self.catalog = yaml.safe_load(f) or {}

        self.glossary = self.catalog.get("glossary", {})
        self.tables = self.catalog.get("tables", {})
        self.examples = self.catalog.get("examples", [])
        self.naming = self.catalog.get("naming_conventions", {})

        self._build_search_index()

    def _build_search_index(self):
        """Construye un índice invertido para búsqueda rápida."""
        self._alias_to_table = {}
        self._keyword_to_tables = {}

        for table_name, table_info in self.tables.items():
            aliases = table_info.get("aliases", [])
            for alias in aliases:
                self._alias_to_table[alias.lower()] = table_name

            keywords = set()
            keywords.add(table_name.lower())
            for alias in aliases:
                keywords.add(alias.lower())
                for word in alias.lower().split():
                    keywords.add(word)

            desc = table_info.get("description", "")
            for word in desc.lower().split():
                if len(word) > 3:
                    keywords.add(word)

            for kw in keywords:
                if kw not in self._keyword_to_tables:
                    self._keyword_to_tables[kw] = []
                self._keyword_to_tables[kw].append(table_name)

        for term, meaning in self.glossary.items():
            term_lower = term.lower()
            if term_lower in self._alias_to_table:
                continue
            for table_name, table_info in self.tables.items():
                aliases = [a.lower() for a in table_info.get("aliases", [])]
                desc = table_info.get("description", "").lower()
                table_parts = set(table_name.lower().split("_"))
                if term_lower in aliases or term_lower in table_parts:
                    self._alias_to_table[term_lower] = table_name
                    break
                if len(term_lower) > 3 and term_lower in desc:
                    self._alias_to_table[term_lower] = table_name
                    break

    def get_relevant_context(self, question: str) -> str:
        """
        Dado una pregunta en lenguaje natural, retorna TODO el contexto
        que la IA necesita para responderla correctamente.

        Este es EL método clave de todo el sistema.
        """
        question_lower = question.lower()
        words = self._tokenize(question_lower)

        relevant_tables = self._find_relevant_tables(question_lower, words)
        similar_examples = self._find_similar_examples(question_lower, words)

        lines = []

        lines.append("=== CONTEXTO SEMÁNTICO PARA TU PREGUNTA ===\n")

        glossary_matches = self._match_glossary(question_lower, words)
        if glossary_matches:
            lines.append("GLOSARIO RELEVANTE:")
            for term, meaning in glossary_matches.items():
                lines.append(f"  {term} = {meaning}")
            lines.append("")

        if relevant_tables:
            lines.append("TABLAS RELEVANTES:")
            for table_name in relevant_tables:
                lines.append(self._format_table_context(table_name))
            lines.append("")

        if similar_examples:
            lines.append("EJEMPLOS SIMILARES:")
            for ex in similar_examples[:3]:
                lines.append(f"  Pregunta: {ex['question']}")
                lines.append(f"  SQL: {ex['sql']}")
                if ex.get("explanation"):
                    lines.append(f"  Nota: {ex['explanation']}")
                lines.append("")

        if not relevant_tables and not similar_examples:
            lines.append(
                "No se encontró contexto específico en el catálogo.\n"
                "Usa las herramientas de exploración (search_tables, describe_table) "
                "para descubrir la estructura.\n"
                "CONSEJO: Agrega esta tabla/concepto al catálogo semántico (catalog.yaml) "
                "para futuras consultas."
            )

        return "\n".join(lines)

    def _tokenize(self, text: str) -> list[str]:
        """Tokeniza texto eliminando ruido."""
        stopwords = {
            "el", "la", "los", "las", "de", "del", "en", "un", "una",
            "que", "es", "por", "con", "para", "al", "se", "su", "como",
            "más", "ya", "este", "esta", "estos", "estas", "cual", "cuál",
            "cuales", "cuáles", "donde", "dónde", "cuando", "cuándo",
            "cuanto", "cuánto", "cuantos", "cuántos", "tiene", "son",
            "hay", "fue", "ser", "hacer", "tener", "ver", "dar", "saber",
            "no", "si", "sí", "pero", "o", "y", "ni", "cada", "todo",
            "toda", "todos", "todas", "otro", "otra", "otros", "otras",
            "mi", "tu", "nos", "les", "me", "te",
        }
        words = []
        for word in text.split():
            cleaned = word.strip("¿?¡!.,;:()[]{}\"'")
            if cleaned and len(cleaned) > 1 and cleaned not in stopwords:
                words.append(cleaned)
        return words

    def _find_relevant_tables(
        self, question: str, words: list[str]
    ) -> list[str]:
        """Encuentra tablas relevantes para la pregunta."""
        found = set()
        scores: dict[str, int] = {}

        for word in words:
            if word in self._alias_to_table:
                table = self._alias_to_table[word]
                found.add(table)
                scores[table] = scores.get(table, 0) + 20

            if word in self._keyword_to_tables:
                for table in self._keyword_to_tables[word]:
                    found.add(table)
                    table_info = self.tables.get(table, {})
                    has_aliases = bool(table_info.get("aliases"))
                    is_enriched = (
                        table_info.get("description", "").startswith("TODO") is False
                        and table_info.get("description", "") != ""
                    )
                    base = 8 if (has_aliases or is_enriched) else 2
                    scores[table] = scores.get(table, 0) + base

        for bigram_i in range(len(words) - 1):
            bigram = words[bigram_i] + " " + words[bigram_i + 1]
            if bigram in self._alias_to_table:
                table = self._alias_to_table[bigram]
                found.add(table)
                scores[table] = scores.get(table, 0) + 25

        primary_tables = {t for t, s in scores.items() if s >= 8}

        for table_name in primary_tables:
            table_info = self.tables.get(table_name, {})
            for rel in table_info.get("relationships", []):
                target = rel.get("target", "")
                if target in self.tables:
                    found.add(target)
                    scores[target] = scores.get(target, 0) + 6

        ranked = sorted(found, key=lambda t: scores.get(t, 0), reverse=True)

        max_tables = 6
        if len(ranked) > max_tables:
            threshold = scores.get(ranked[0], 0) * 0.15
            ranked = [t for t in ranked if scores.get(t, 0) >= threshold][:max_tables]

        return ranked

    def _format_table_context(self, table_name: str) -> str:
        """Formatea el contexto completo de una tabla."""
        info = self.tables.get(table_name, {})
        lines = []

        lines.append(f"\n  --- {table_name} ---")
        if info.get("description"):
            lines.append(f"  Descripción: {info['description']}")
        if info.get("aliases"):
            lines.append(f"  También conocida como: {', '.join(info['aliases'])}")

        if info.get("columns"):
            lines.append("  Columnas clave:")
            for col_name, col_desc in info["columns"].items():
                lines.append(f"    {col_name}: {col_desc}")

        if info.get("relationships"):
            lines.append("  Relaciones:")
            for rel in info["relationships"]:
                lines.append(
                    f"    -> {rel['target']} ({rel.get('type', '?')}): "
                    f"{rel.get('description', '')}"
                )
                if rel.get("join"):
                    lines.append(f"       JOIN: {rel['join']}")

        if info.get("calculations"):
            lines.append("  Cálculos comunes:")
            for calc_name, calc_formula in info["calculations"].items():
                lines.append(f"    {calc_name}: {calc_formula}")

        return "\n".join(lines)

    def _match_glossary(
        self, question: str, words: list[str]
    ) -> dict[str, str]:
        """Encuentra términos del glosario mencionados en la pregunta."""
        matches = {}
        for term, meaning in self.glossary.items():
            if term.lower() in question or term.lower() in words:
                matches[term] = meaning
        return matches

    def _find_similar_examples(
        self, question: str, words: list[str]
    ) -> list[dict]:
        """Encuentra ejemplos con preguntas similares."""
        scored = []
        for ex in self.examples:
            ex_words = set(self._tokenize(ex["question"].lower()))
            overlap = len(set(words) & ex_words)
            if overlap >= 2:
                scored.append((overlap, ex))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [ex for _, ex in scored[:3]]

    def list_all_aliases(self) -> dict[str, list[str]]:
        """Lista todos los alias definidos en el catálogo."""
        result = {}
        for table_name, info in self.tables.items():
            aliases = info.get("aliases", [])
            if aliases:
                result[table_name] = aliases
        return result

    def get_table_info(self, table_name: str) -> dict | None:
        """Retorna la info completa de una tabla del catálogo."""
        return self.tables.get(table_name)

    def get_all_calculations(self) -> dict[str, dict]:
        """Retorna todos los cálculos definidos en el catálogo."""
        result = {}
        for table_name, info in self.tables.items():
            calcs = info.get("calculations", {})
            if calcs:
                result[table_name] = calcs
        return result
