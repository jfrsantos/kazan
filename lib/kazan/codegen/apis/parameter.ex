defmodule Kazan.Codegen.Apis.Parameter do
  @moduledoc false
  import Kazan.Codegen.Naming, only: [definition_ref_to_model_module: 1]

  defstruct [
    :type,
    :var_name,
    :field_name,
    :description,
    :required,
    :schema,
    :data_type
  ]

  @type t :: %__MODULE__{
          type: atom,
          var_name: atom,
          field_name: String.t(),
          description: String.t(),
          required: boolean | nil,
          schema: atom | nil,
          data_type: atom | nil
        }

  @spec from_oai_desc(map()) :: t
  def from_oai_desc(desc) do
    %__MODULE__{
      type: parse_type(desc["in"]),
      var_name: Macro.underscore(desc["name"]) |> String.to_atom(),
      field_name: desc["name"],
      description: desc["description"],
      required: parse_required(desc["in"], desc["required"]),
      schema: definition_ref_to_model_module(get_in(desc, ["schema", "$ref"])),
      data_type: desc["type"]
    }
  end

  @spec parse_type(String.t()) :: atom
  defp parse_type("body"), do: :body
  defp parse_type("path"), do: :path
  defp parse_type("query"), do: :query

  @spec parse_required(String.t(), boolean | nil) :: boolean
  defp parse_required("body", _), do: true
  defp parse_required("path", _), do: true
  defp parse_required(_, true), do: true
  defp parse_required(_, _), do: false
end
