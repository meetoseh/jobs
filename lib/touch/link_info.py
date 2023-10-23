import json
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, validator

from lib.shared.redis_hash import RedisHash


class TouchLink(BaseModel):
    uid: str = Field(
        description="the unique identifier assigned to this touch link, which "
        "will correspond to the uid in the database if/when persisted"
    )
    code: str = Field(description="the code sent to the user in the touch")
    touch_uid: str = Field(
        description="the send_uid of the touch that this link is associated with"
    )
    page_identifier: str = Field(
        description="the identifier for what page the user should be sent to "
        "when they provide the code"
    )
    page_extra: Dict[str, Any] = Field(
        description="extra keyword arguments for the page, which depends on the page"
    )
    preview_identifier: str = Field(
        description="the identifier used to form the open graph meta tags for the page "
        "when the code is embedded in a link"
    )
    preview_extra: Dict[str, Any] = Field(
        description="extra keyword arguments for the preview, which depends on the preview"
    )
    created_at: float = Field(
        description="the time at which the touch link was created in the buffer sorted set"
    )

    @classmethod
    def from_redis_hget(cls, args: List[Union[str, bytes]]):
        """Parses the result from the hget command which requests all keys in the order
        defined in this class
        """
        return cls(
            uid=args[0] if isinstance(args[0], str) else args[0].decode("utf-8"),
            code=args[1] if isinstance(args[1], str) else args[1].decode("utf-8"),
            touch_uid=args[2] if isinstance(args[2], str) else args[2].decode("utf-8"),
            page_identifier=args[3]
            if isinstance(args[3], str)
            else args[3].decode("utf-8"),
            page_extra=json.loads(args[4]),
            preview_identifier=args[5]
            if isinstance(args[5], str)
            else args[5].decode("utf-8"),
            preview_extra=json.loads(args[6]),
            created_at=float(args[7]),
        )

    def as_redis_mapping(self) -> Dict[bytes, bytes]:
        """Converts this information into a dictionary that can be used
        as the mapping for hset
        """
        return {
            b"uid": self.uid.encode("utf-8"),
            b"code": self.code.encode("utf-8"),
            b"touch_uid": self.touch_uid.encode("utf-8"),
            b"page_identifier": self.page_identifier.encode("utf-8"),
            b"page_extra": json.dumps(self.page_extra).encode("utf-8"),
            b"preview_identifier": self.preview_identifier.encode("utf-8"),
            b"preview_extra": json.dumps(self.preview_extra).encode("utf-8"),
            b"created_at": str(self.created_at).encode("utf-8"),
        }

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "TouchLink":
        """Parses the result from hgetall (or similar) for the data in
        `touch_links:buffer:{code}` into the corresponding object.
        """
        data = RedisHash(mapping_raw)
        return cls(
            uid=data.get_str(b"uid"),
            code=data.get_str(b"code"),
            touch_uid=data.get_str(b"touch_uid"),
            page_identifier=data.get_str(b"page_identifier"),
            page_extra=json.loads(data.get_bytes(b"page_extra")),
            preview_identifier=data.get_str(b"preview_identifier"),
            preview_extra=json.loads(data.get_bytes(b"preview_extra")),
            created_at=data.get_float(b"created_at"),
        )


class TouchLinkBufferedClick(BaseModel):
    uid: str = Field(
        description="the click uid assigned to this click, which will correspond to the "
        "user touch click uid in the database if/when persisted"
    )
    clicked_at: float = Field(
        description="when the click was received by the server, in seconds since the "
        "epoch"
    )
    visitor_uid: Optional[str] = Field(
        description="the uid of the visitor that clicked the link, if known"
    )
    user_sub: Optional[str] = Field(
        description="the sub of the user who clicked the link, if known"
    )
    track_type: Literal["on_click", "post_login"] = Field(
        description="the type of track that was sent to the server"
    )
    parent_uid: Optional[str] = Field(
        description="iff track_type is post_login, the uid of the click that was "
        "originally sent to the server that is being augmented by this track"
    )

    @validator("parent_uid")
    def parent_uid_must_be_present_if_track_type_is_post_login(cls, v, values):
        if values["track_type"] == "post_login" and v is None:
            raise ValueError("parent_uid must be present if track_type is post_login")
        if values["track_type"] != "post_login" and v is not None:
            raise ValueError(
                "parent_uid must not be present if track_type is not post_login"
            )
        return v


class TouchLinkUidIndex(BaseModel):
    code: str = Field(description="the code for the link")
    has_child: bool = Field(
        description="True iff the track_type is on_click and a post_login track has been received "
        "for this link already"
    )

    def as_redis_mapping(self) -> Dict[bytes, bytes]:
        """Converts this information into a dictionary that can be used
        as the mapping for hset
        """
        return {
            b"code": self.code.encode("utf-8"),
            b"has_child": b"1" if self.has_child else b"0",
        }

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "TouchLinkUidIndex":
        """Parses the result from hgetall (or similar) for the data in the uid
        index (touch_links:buffer:on_clicks_by_uid:{uid}) into the
        corresponding object.
        """
        data = RedisHash(mapping_raw)
        return cls(
            code=data.get_str(b"code"),
            has_child=data.get_int(b"has_child", default=0) == 1,
        )


class TouchLinkDelayedClick(BaseModel):
    uid: str = Field(
        description="the click uid assigned to this click, which will correspond to the "
        "user touch click uid in the database if/when persisted"
    )
    link_code: str = Field(
        description="the code sent to the user in the touch that this click is associated with"
    )
    track_type: Literal["on_click", "post_login"] = Field(
        description="the type of track that was sent to the server"
    )
    parent_uid: Optional[str] = Field(
        description="iff track_type is post_login, the uid of the click that was "
        "originally sent to the server that is being augmented by this track"
    )
    user_sub: Optional[str] = Field(
        description="the sub of the user who clicked the link, if known"
    )
    visitor_uid: Optional[str] = Field(
        description="the uid of the visitor that clicked the link, if known"
    )
    clicked_at: float = Field(
        description="when the click was received by the server, in seconds since the "
        "epoch"
    )

    @validator("parent_uid")
    def parent_uid_must_be_present_if_track_type_is_post_login(cls, v, values):
        if values["track_type"] == "post_login" and v is None:
            raise ValueError("parent_uid must be present if track_type is post_login")
        if values["track_type"] != "post_login" and v is not None:
            raise ValueError(
                "parent_uid must not be present if track_type is not post_login"
            )
        return v

    def as_redis_mapping(self) -> Dict[bytes, bytes]:
        return {
            b"uid": self.uid.encode("utf-8"),
            b"link_code": self.link_code.encode("utf-8"),
            b"track_type": self.track_type.encode("utf-8"),
            b"parent_uid": self.parent_uid.encode("utf-8")
            if self.parent_uid is not None
            else b"",
            b"user_sub": self.user_sub.encode("utf-8")
            if self.user_sub is not None
            else b"",
            b"visitor_uid": self.visitor_uid.encode("utf-8")
            if self.visitor_uid is not None
            else b"",
            b"clicked_at": str(self.clicked_at).encode("utf-8"),
        }

    @classmethod
    def from_redis_mapping(
        cls,
        mapping_raw: Union[
            List[Union[str, bytes]],
            Dict[Union[str, bytes], Union[str, bytes]],
        ],
    ) -> "TouchLinkDelayedClick":
        """Parses the result from hgetall (or similar) for the data in
        `touch_links:delayed_clicks:{code}` into the corresponding object.
        """
        data = RedisHash(mapping_raw)
        return cls(
            uid=data.get_str(b"uid"),
            link_code=data.get_str(b"link_code"),
            track_type=data.get_str(b"track_type"),
            parent_uid=data.get_str(b"parent_uid", default=None),
            user_sub=data.get_str(b"user_sub", default=None),
            visitor_uid=data.get_str(b"visitor_uid", default=None),
            clicked_at=data.get_float(b"clicked_at"),
        )
