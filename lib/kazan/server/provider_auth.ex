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
    import Inspect.Algebra

    def inspect(auth, opts) do
      concat([
        "#ProviderAuth<config: ",
        inspect_config(auth.config, opts),
        ", expiry: #{DateTime.to_string(auth.expiry)})>"
      ])
    end

    defp inspect_config(config, opts) do
      config
      |> Map.drop([:env])
      |> to_doc(opts)
    end
  end
end
