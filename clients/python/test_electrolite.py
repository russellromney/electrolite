import unittest

from electrolite import HttpResponse, ShapeClient


class ShapeClientTests(unittest.TestCase):
    def test_materializes_snapshot_and_replay(self):
        client = ShapeClient("http://app.test/electrolite/v1/projectTodos/p1", key_columns=["id"])
        seen = []
        client.subscribe(lambda rows: seen.append(rows))

        self.assertTrue(
            client.apply(
                {
                    "type": "snapshot",
                    "key_columns": ["id"],
                    "rows": [{"id": 1, "title": "ship", "done": 0}],
                    "offset": 2,
                    "up_to_date": True,
                }
            )
        )
        self.assertEqual(client.current_rows(), [{"id": 1, "title": "ship", "done": 0}])
        self.assertEqual(client.offset, 2)

        self.assertTrue(
            client.apply(
                {
                    "type": "replay",
                    "messages": [
                        {
                            "type": "update",
                            "key": {"id": 1},
                            "value": {"id": 1, "title": "ship it", "done": 1},
                            "offset": 3,
                        },
                        {
                            "type": "insert",
                            "key": {"id": 2},
                            "value": {"id": 2, "title": "demo", "done": 0},
                            "offset": 4,
                        },
                    ],
                    "offset": 4,
                    "up_to_date": True,
                }
            )
        )
        self.assertEqual(
            client.current_rows(),
            [
                {"id": 1, "title": "ship it", "done": 1},
                {"id": 2, "title": "demo", "done": 0},
            ],
        )
        self.assertEqual(len(seen), 3)

    def test_stages_replay_until_up_to_date(self):
        client = ShapeClient("http://app.test/electrolite/v1/projectTodos/p1", key_columns=["id"])
        client.apply(
            {
                "type": "snapshot",
                "key_columns": ["id"],
                "rows": [{"id": 1, "title": "old"}],
                "offset": 1,
                "up_to_date": True,
            }
        )

        changed = client.apply(
            {
                "type": "replay",
                "messages": [
                    {
                        "type": "update",
                        "key": {"id": 1},
                        "value": {"id": 1, "title": "new"},
                        "offset": 2,
                    }
                ],
                "offset": 2,
                "up_to_date": False,
            }
        )
        self.assertFalse(changed)
        self.assertEqual(client.current_rows(), [{"id": 1, "title": "old"}])

        changed = client.apply(
            {
                "type": "replay",
                "messages": [],
                "offset": 2,
                "up_to_date": True,
            }
        )
        self.assertTrue(changed)
        self.assertEqual(client.current_rows(), [{"id": 1, "title": "new"}])

    def test_request_handles_204_and_resync(self):
        responses = [
            HttpResponse(204),
            HttpResponse(409, {"error": "resync_required"}),
            HttpResponse(
                200,
                {
                    "type": "snapshot",
                    "key_columns": ["id"],
                    "rows": [{"id": 7, "title": "fresh"}],
                    "offset": 9,
                    "up_to_date": True,
                },
            ),
        ]
        requested = []

        def fetch(url):
            requested.append(url)
            return responses.pop(0)

        client = ShapeClient("http://app.test/electrolite/v1/projectTodos/p1", fetch=fetch)
        self.assertFalse(client.request(offset=10, live=True))
        self.assertEqual(
            requested[0],
            "http://app.test/electrolite/v1/projectTodos/p1?offset=10&live=true",
        )

        self.assertTrue(client.request(offset=10, live=True))
        self.assertEqual(client.current_rows(), [{"id": 7, "title": "fresh"}])
        self.assertEqual(
            requested[-1],
            "http://app.test/electrolite/v1/projectTodos/p1?offset=-1",
        )


if __name__ == "__main__":
    unittest.main()
