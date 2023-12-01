defmodule Kazan.Server.ProviderAuth do
  @moduledoc """
  This struct stores details of how to auth with an auth provider.

  This is used to support authorization with GKE using gcloud for example.
  """
  alias Kazan.Server.TokenAuth

  @enforce_keys [:config]
  defstruct [:config, :token, :expiry]

  @typep config_t :: %{
           cmd_path: String.t(),
           cmd_args: [String.t()],
           token_key_path: [String.t()],
           expiry_key_path: [String.t()],
           env: [{String.t(), String.t()}] | nil
         }

  @type t :: %__MODULE__{
          config: config_t,
          token: TokenAuth.t() | nil,
          expiry: DateTime.t() | nil
        }

  defimpl Inspect do
    def inspect(auth, opts) do
      output =
        "~w(config: #{inspect_config(auth.config)}, token: ******, expiry: #{DateTime.to_string(auth.expiry)})"

      Inspect.Map.inspect(output, opts)
    end

    defp inspect_config(config) do
      env = replace_env_value(config.env)

      Map.put_new(config, :env, env)
      |> Map.from_struct()
      |> Map.replace(:env, inspect(env))
    end

    defp replace_env_value(env) do
      sensitive_env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN"
      ]

      Enum.map(env, fn {key, value} ->
        if Enum.member?(sensitive_env_vars, key) do
          {key, "******"}
        else
          {key, value}
        end
      end)
      |> Map.new()
    end
  end
end
