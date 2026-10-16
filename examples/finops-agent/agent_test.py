# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Unit tests for agent.py's usage conversion: python3 -m unittest agent_test.py"""

import unittest

from google.genai import types

import agent


class UsageFromMetadataTest(unittest.TestCase):
    def test_semconv_inclusion(self):
        um = types.GenerateContentResponseUsageMetadata(
            prompt_token_count=4000,  # includes the 3000 cached tokens
            cached_content_token_count=3000,
            candidates_token_count=200,
            thoughts_token_count=500,
            total_token_count=4700,
        )
        self.assertEqual(
            agent.usage_from_metadata(um),
            {"input": 4000, "output": 700, "cacheRead": 3000, "cacheWrite": 0, "reasoning": 500},
        )

    def test_missing_fields_are_zero(self):
        um = types.GenerateContentResponseUsageMetadata(prompt_token_count=13, candidates_token_count=42)
        self.assertEqual(
            agent.usage_from_metadata(um),
            {"input": 13, "output": 42, "cacheRead": 0, "cacheWrite": 0, "reasoning": 0},
        )
        self.assertEqual(
            agent.usage_from_metadata(None),
            {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "reasoning": 0},
        )


if __name__ == "__main__":
    unittest.main()
