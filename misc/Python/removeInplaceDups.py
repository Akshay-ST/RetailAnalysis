class Solution:
    def removeDuplicates(self, nums: list[int]) -> int:
        if not nums:
            return 0

        # k tracks the write position for unique elements
        k = 0

        # Compare element i with element i + 1
        for i in range(len(nums) - 1):
            if nums[i] != nums[i + 1]:
                nums[k] = nums[i]
                k += 1

        # The last element is always unique among what came before it
        nums[k] = nums[-1]
        k += 1

        return k