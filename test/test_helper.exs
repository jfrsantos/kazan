ExUnit.start()
ExUnit.configure capture_log: false, exclude: [:integration]
Application.ensure_all_started(:bypass)
