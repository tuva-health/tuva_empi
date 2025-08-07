"""Pagination utilities for API views."""

from typing import Any, Dict, List

from rest_framework import status
from rest_framework.response import Response


class PaginationMixin:
    """Mixin to add pagination functionality to views."""

    DEFAULT_PAGE_SIZE = 50
    MAX_PAGE_SIZE = 1000

    def get_pagination_params(self, data: Dict[str, Any]) -> tuple[int, int]:
        """Extract pagination parameters from request data."""
        page = data.get("page", 1)
        page_size = min(
            data.get("page_size", self.DEFAULT_PAGE_SIZE), self.MAX_PAGE_SIZE
        )
        return page, page_size

    def paginate_list(
        self, items: List[Any], page: int, page_size: int
    ) -> Dict[str, Any]:
        """Paginate a list of items and return pagination metadata."""
        total_count = len(items)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_items = items[start_index:end_index]

        total_pages = (total_count + page_size - 1) // page_size
        has_next = page < total_pages
        has_previous = page > 1

        return {
            "items": paginated_items,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_count": total_count,
                "total_pages": total_pages,
                "has_next": has_next,
                "has_previous": has_previous,
                "next_page": page + 1 if has_next else None,
                "previous_page": page - 1 if has_previous else None,
            },
        }

    def create_paginated_response(
        self, items: List[Any], page: int, page_size: int, response_key: str = "items"
    ) -> Response:
        """Create a paginated response with standard format."""
        paginated_data = self.paginate_list(items, page, page_size)

        return Response(
            {
                response_key: paginated_data["items"],
                "pagination": paginated_data["pagination"],
            },
            status=status.HTTP_200_OK,
        )

    def create_simple_response(
        self, items: List[Any], response_key: str = "items"
    ) -> Response:
        """Create a simple response without pagination metadata."""
        return Response(
            {response_key: items},
            status=status.HTTP_200_OK,
        )
