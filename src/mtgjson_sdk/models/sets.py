"""MTGJSON set models (vendored, standalone)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from .cards import CardSet, CardSetDeck, CardToken
from .sealed import SealedProduct
from .submodels import BoosterConfig, Translations


class DeckSet(BaseModel):
    """Deck with minimal card references (as in Set.decks)."""

    model_config = {"populate_by_name": True}

    code: str = Field(description="The printing set code for the deck.")
    name: str = Field(description="The name of the deck.")
    type: str = Field(description="The type of deck.")
    release_date: str | None = Field(default=None, alias="releaseDate")
    sealed_product_uuids: list[str] | None = Field(
        default=None, alias="sealedProductUuids"
    )
    main_board: list[CardSetDeck] = Field(default_factory=list, alias="mainBoard")
    side_board: list[CardSetDeck] = Field(default_factory=list, alias="sideBoard")
    commander: list[CardSetDeck] | None = Field(default=None)
    display_commander: list[CardSetDeck] | None = Field(
        default=None, alias="displayCommander"
    )
    tokens: list[CardSetDeck] | None = None
    planes: list[CardSetDeck] | None = None
    schemes: list[CardSetDeck] | None = None
    source_set_codes: list[str] | None = Field(default=None, alias="sourceSetCodes")


class SetList(BaseModel):
    """Set summary metadata (without individual cards).

    Used by :meth:`~mtgjson_sdk.queries.sets.SetQuery.get` and
    :meth:`~mtgjson_sdk.queries.sets.SetQuery.list`. Contains set code,
    name, type, release date, sizes, and marketplace IDs.
    """

    model_config = {"populate_by_name": True}

    code: str = Field(description="The printing set code for the set.")
    name: str = Field(description="The name of the set.")
    type: str = Field(description="The expansion type of the set.")
    release_date: str = Field(alias="releaseDate")
    base_set_size: int = Field(alias="baseSetSize")
    total_set_size: int = Field(alias="totalSetSize")
    keyrune_code: str = Field(alias="keyruneCode")
    translations: Translations = Field(default_factory=dict)  # type: ignore[assignment]

    block: str | None = Field(default=None)
    parent_code: str | None = Field(default=None, alias="parentCode")
    mtgo_code: str | None = Field(default=None, alias="mtgoCode")
    token_set_code: str | None = Field(default=None, alias="tokenSetCode")

    mcm_id: int | None = Field(default=None, alias="mcmId")
    mcm_id_extras: int | None = Field(default=None, alias="mcmIdExtras")
    mcm_name: str | None = Field(default=None, alias="mcmName")
    tcgplayer_group_id: int | None = Field(default=None, alias="tcgplayerGroupId")
    cardsphere_set_id: int | None = Field(default=None, alias="cardsphereSetId")

    is_foil_only: bool = Field(default=False, alias="isFoilOnly")
    is_non_foil_only: bool | None = Field(default=None, alias="isNonFoilOnly")
    is_online_only: bool = Field(default=False, alias="isOnlineOnly")
    is_paper_only: bool | None = Field(default=None, alias="isPaperOnly")
    is_foreign_only: bool | None = Field(default=None, alias="isForeignOnly")
    is_partial_preview: bool | None = Field(default=None, alias="isPartialPreview")

    languages: list[str] | None = Field(default=None)
    decks: list[DeckSet] | None = Field(default=None)
    sealed_product: list[SealedProduct] | None = Field(
        default=None, alias="sealedProduct"
    )


class MtgSet(SetList):
    """Full set with cards, tokens, decks, and booster configuration."""

    cards: list[CardSet] = Field(default_factory=list)
    tokens: list[CardToken] = Field(default_factory=list)
    booster: dict[str, BoosterConfig] | None = Field(default=None)
